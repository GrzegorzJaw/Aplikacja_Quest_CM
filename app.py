# app.py
# -*- coding: utf-8 -*-
"""
Streamlit app that:
1) Logs in (3 roles: ksiegowosc, krzysztof, admin)
2) Watches a Google Drive folder for new PDFs
3) Extracts key fields from Polish/EN invoices/receipts (issuer, amount, netto, brutto, description, due date)
4) Appends rows to a DataFrame and an Excel file in the *same Drive folder*
5) Allows toggling "Zapłacone?" (only ksiegowosc can edit; krzysztof has read‑only; admin is read‑only and cannot see amounts)
6) Tab "Do zapłaty na dzisiaj" shows unpaid items due today or earlier

Setup (once):
- In Streamlit secrets, define:
  [gdrive]
  folder_id = "<YOUR_FOLDER_ID>"
  service_account_json = "{""type"": ""service_account"", ...}"

  [users]
  ksiegowosc_password = "<password>"
  krzysztof_password = "<password>"
  admin_password = "<password>"

- Python deps (example):
  streamlit, pandas, pdfplumber, google-api-python-client, google-auth, google-auth-httplib2, google-auth-oauthlib, openpyxl, pdf2image, pillow, pytesseract
  Note: pdf2image requires Poppler; install it on the host for OCR fallback to work.

Security note: This sample uses very simple password checks from st.secrets.
In production, replace with proper auth (e.g., SSO, streamlit-authenticator with hashed passwords, etc.).
"""

from __future__ import annotations
import io
import os
import re
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd

# PDF text/ocr
import pdfplumber
try:
    from pdf2image import convert_from_bytes
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# Google Drive API
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from collections.abc import Mapping
from datetime import datetime as _dt
import bcrypt

APP_TITLE = "Faktury – monitor faktur QUEST"
EXCEL_BASENAME = "faktury.xlsx"  # kept in the same Drive folder
STATE_FILENAME = "state.json"     # tracks seen file IDs (also in Drive folder)


REQUIRED_COLUMNS = [
    "file_id", "nazwa_dokumentu", "Data wprowadzenia rachunku", "opis",
    "kwota", "netto", "brutto", "termin_platnosci", "zaplacone"
]


# ---------- UTIL: AUTH ----------
def get_role_from_login():
    """
    Zwraca rolę zalogowanego użytkownika albo None (gdy nie zalogowany).
    1) Jeśli Streamlit SSO poda email – wstawiamy go do formularza.
    2) Weryfikujemy hasło przez users.json na Google Drive (verify_user_password).
    3) Rate-limit: 5 nieudanych prób → blokada na 60s.
    """
    # --- SSO z Streamlit Cloud (opcjonalnie) ---
    sso_user = getattr(st, "experimental_user", None)
    sso_email = getattr(sso_user, "email", None)

    # --- Session: blokada po wielu błędach ---
    fails = st.session_state.get("login_fails", 0)
    lock_until = st.session_state.get("login_lock_until", 0)
    now = time.time()
    if now < lock_until:
        remaining = int(lock_until - now)
        st.error(f"Zbyt wiele prób logowania. Spróbuj ponownie za {remaining}s.")
        return None

    # --- Jeśli już zalogowany w tej sesji, zwróć rolę ---
    if "role" in st.session_state and "user_email" in st.session_state:
        return st.session_state["role"]

    # --- Formularz logowania (email + hasło) ---
    st.subheader("Logowanie")
    email = st.text_input("E-mail", value=sso_email or "", key="login_email").strip()
    password = st.text_input("Hasło", type="password", key="login_password")

    col_l, col_r = st.columns([1, 1])
    with col_l:
        ok = st.button("Zaloguj", type="primary", key="btn_login")
    with col_r:
        if st.button("Wyloguj", key="btn_logout"):
            for k in ("role", "user_email", "login_fails", "login_lock_until"):
                st.session_state.pop(k, None)
            st.experimental_rerun()

    if not ok:
        return None

    # --- Weryfikacja po stronie Drive (users.json) ---
    try:
        service, folder_id = drive_service()  # potrzebny do odczytu users.json
    except Exception as e:
        st.error(f"Błąd inicjalizacji Drive: {e}")
        return None

    role = verify_user_password(service, folder_id, email, password)
    if role:
        # sukces logowania
        st.session_state["role"] = role
        st.session_state["user_email"] = email
        st.session_state["login_fails"] = 0
        st.session_state["login_lock_until"] = 0
        st.success(f"Zalogowano jako {role}")
        return role
    else:
        # porażka logowania → inkrementuj i ewentualnie zablokuj
        fails += 1
        st.session_state["login_fails"] = fails
        if fails >= 5:
            st.session_state["login_lock_until"] = time.time() + 60  # 60s blokady
            st.session_state["login_fails"] = 0
            st.error("Zbyt wiele nieudanych prób. Zablokowano na 60 sekund.")
        else:
            st.error("Błędny e-mail lub hasło.")
        return None

# ---------- UTIL: DRIVE ----------
def drive_service():
    cfg = st.secrets.get("gdrive", None)
    if not cfg:
        st.error("Brak sekcji [gdrive] w .streamlit/secrets.toml")
        st.stop()

    folder_id = cfg.get("folder_id")
    if not folder_id:
        st.error("Brak gdrive.folder_id w secrets")
        st.stop()

    sa_json = cfg.get("service_account_json")
    if not sa_json:
        st.error("Brak gdrive.service_account_json w secrets")
        st.stop()

    # Akceptuj zarówno tabelę TOML (Mapping/AttrDict), jak i string JSON
    try:
        info = dict(sa_json) if isinstance(sa_json, Mapping) else json.loads(sa_json)
    except Exception as e:
        st.error(f"Nieprawidłowy service_account_json w secrets: {e}")
        st.stop()

    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    service = build("drive", "v3", credentials=creds)
    return service, folder_id


def list_pdfs(service, folder_id) -> List[Dict[str, Any]]:
    query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, mimeType, createdTime, modifiedTime)").execute()
    return results.get("files", [])


def find_file_by_name(service, folder_id, name: str) -> Optional[Dict[str, Any]]:
    # Escape single quotes for Drive query
    safe_name = name.replace("'", "\\'")
    query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    r = service.files().list(q=query, fields="files(id, name)").execute()
    files = r.get("files", [])
    return files[0] if files else None


def download_bytes(service, file_id: str) -> bytes:
    req = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue()


def upload_or_update_file(service, folder_id: str, name: str, data: bytes, mime: str) -> str:
    existing = find_file_by_name(service, folder_id, name)
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    if existing:
        file_id = existing["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        return file_id
    else:
        meta = {"name": name, "parents": [folder_id]}
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        return f["id"]

# ---------- STATE (seen PDFs) ----------

def load_state(service, folder_id) -> Dict[str, Any]:
    f = find_file_by_name(service, folder_id, STATE_FILENAME)
    if not f:
        return {"seen": []}
    data = download_bytes(service, f["id"]) or b"{}"
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {"seen": []}


def save_state(service, folder_id, state: Dict[str, Any]):
    payload = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    upload_or_update_file(service, folder_id, STATE_FILENAME, payload, "application/json")


def current_drive_ids(service, folder_id) -> set:
    return {f["id"] for f in list_pdfs(service, folder_id)}


def sync_with_drive(service, folder_id, df: pd.DataFrame, seen_ids: set) -> tuple[pd.DataFrame, set, int]:
    """Synchronize DF/state with current files on Drive.
    Removes rows for files that no longer exist in the folder and updates state.
    Returns: (new_df, new_seen_ids, removed_count)
    """
    ids_now = current_drive_ids(service, folder_id)
    # rows to keep are those still present on Drive
    keep_mask = df["file_id"].isin(ids_now) if "file_id" in df.columns else pd.Series([], dtype=bool)
    removed = int((~keep_mask).sum()) if len(df) else 0
    new_df = df.loc[keep_mask].copy() if len(df) else df
    new_seen = seen_ids.intersection(ids_now)
    # persist
    save_excel_df(service, folder_id, new_df)
    save_state(service, folder_id, {"seen": list(new_seen)})
    return new_df, new_seen, removed

# ---------- EXCEL STORE ----------

def load_excel_df(service, folder_id) -> pd.DataFrame:
    f = find_file_by_name(service, folder_id, EXCEL_BASENAME)
    if not f:
        df = pd.DataFrame(columns=REQUIRED_COLUMNS)
        return df
    data = download_bytes(service, f["id"])
    try:
        df = pd.read_excel(io.BytesIO(data))
    except Exception:
        df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    # Ensure schema
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = pd.Series(dtype="object")
    # normalize types
    if "zaplacone" in df.columns:
        df["zaplacone"] = df["zaplacone"].fillna(False).astype(bool)
    return df[REQUIRED_COLUMNS]


def save_excel_df(service, folder_id, df: pd.DataFrame):
    buf = io.BytesIO()
    # ensure boolean as proper type
    out = df.copy()
    if "zaplacone" in out.columns:
        out["zaplacone"] = out["zaplacone"].astype(bool)
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        out.to_excel(writer, index=False)
    upload_or_update_file(service, folder_id, EXCEL_BASENAME, buf.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- INVOICE PARSING ----------

AMOUNT_PAT = re.compile(r"(?P<val>\d{1,3}(?:[\s\u00A0]?\d{3})*(?:[\.,]\d{2})?)\s*(PLN|zł|zl|PLN\b)?", re.IGNORECASE)
NET_LABELS = ["netto", "net", "kwota netto", "wartość netto"]
GROSS_LABELS = ["brutto", "gross", "kwota brutto", "wartość brutto"]
DUE_PATTERNS = [
    r"termin\s+płatności[:\s]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
    r"termin\s+płatności[:\s]*([0-9]{2}\.[0-9]{2}\.[0-9]{4})",
    r"termin\s+płatności[:\s]*([0-9]{2}-[0-9]{2}-[0-9]{4})",
    r"payment\s+due[:\s]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
    r"due\s+date[:\s]*([0-9]{2}\/[0-9]{2}\/[0-9]{4})",
]

SELLER_HINTS = ["sprzedawca", "wystawca", "seller", "issuer", "usługodawca", "dostawca"]
DESC_HINTS = ["opis", "tytuł", "za co", "nazwa usługi", "przedmiot", "description", "item", "service"]


def normalize_text(txt: str) -> str:
    return re.sub(r"[\t\r]+", " ", txt.replace("\xa0", " ")).strip()


def pdf_to_text(pdf_bytes: bytes) -> str:
    # 1) Try text layer via pdfplumber
    try:
        text_blocks = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    text_blocks.append(t)
        if text_blocks:
            return normalize_text("\n".join(text_blocks))
    except Exception:
        pass
    # 2) OCR fallback if available
    if OCR_AVAILABLE:
        try:
            images = convert_from_bytes(pdf_bytes)
            ocr_texts = []
            for im in images:
                ocr_texts.append(pytesseract.image_to_string(im, lang="pol+eng"))
            if any(ocr_texts):
                return normalize_text("\n".join(ocr_texts))
        except Exception:
            return ""
    return ""


def find_first_date(s: str) -> Optional[str]:
    # Support common PL/EN date formats
    for pat in [
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{2}\.\d{2}\.\d{4})",
        r"(\d{2}-\d{2}-\d{4})",
        r"(\d{2}/\d{2}/\d{4})",
    ]:
        m = re.search(pat, s)
        if m:
            raw = m.group(1)
            # normalize to YYYY-MM-DD
            try:
                if "." in raw:
                    d = dt.datetime.strptime(raw, "%d.%m.%Y").date()
                elif "/" in raw:
                    d = dt.datetime.strptime(raw, "%m/%d/%Y").date()
                elif "-" in raw and len(raw) == 10 and raw[4] == "-":
                    d = dt.datetime.strptime(raw, "%Y-%m-%d").date()
                else:
                    d = dt.datetime.strptime(raw, "%d-%m-%Y").date()
                return d.isoformat()
            except Exception:
                continue
    return None


def extract_due_date(text: str) -> Optional[str]:
    for pat in DUE_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            normalized = find_first_date(m.group(1))
            if normalized:
                return normalized
    # heuristic fallback: nearest date after the word "termin"
    m = re.search(r"termin[\w\s:]*?([0-9\./-]{8,10})", text, flags=re.IGNORECASE)
    if m:
        return find_first_date(m.group(1))
    return None


def best_amount_near_labels(text: str, labels: List[str]) -> Optional[float]:
    # search line by line and prefer amounts on lines containing any label
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for l in lines:
        if any(lbl.lower() in l.lower() for lbl in labels):
            m = AMOUNT_PAT.search(l)
            if m:
                val = m.group("val").replace(" ", "").replace("\u00A0", "").replace(",", ".")
                try:
                    return float(val)
                except Exception:
                    pass
    return None


def extract_amounts(text: str) -> Dict[str, Optional[float]]:
    netto = best_amount_near_labels(text, NET_LABELS)
    brutto = best_amount_near_labels(text, GROSS_LABELS)
    kwota = None
    if brutto is not None:
        kwota = brutto
    elif netto is not None:
        kwota = netto
    else:
        # global max numeric as a crude fallback (often total)
        vals = []
        for m in AMOUNT_PAT.finditer(text):
            v = m.group("val").replace(" ", "").replace("\u00A0", "").replace(",", ".")
            try:
                vals.append(float(v))
            except Exception:
                pass
        if vals:
            kwota = max(vals)
    return {"netto": netto, "brutto": brutto, "kwota": kwota}


def extract_seller_and_desc(text: str) -> Dict[str, Optional[str]]:
    # Very simple heuristics: take a block after SELLER_HINTS as seller; first long-ish line near DESC_HINTS as opis
    seller = None
    opis = None
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Seller
    for i, l in enumerate(lines):
        if any(h in l.lower() for h in SELLER_HINTS):
            # pick this line or the next non-empty
            candidate = l
            if i + 1 < len(lines) and len(lines[i+1]) > 3:
                candidate = lines[i+1]
            # clean noisy tails like "NIP ..."
            candidate = re.split(r"\bNIP\b|\bVAT\b|\bREGON\b|\bKRS\b", candidate, flags=re.IGNORECASE)[0].strip(" :,-")
            if len(candidate) >= 3:
                seller = candidate
                break
    # Fallback: first uppercase-heavy line near top
    if not seller:
        for l in lines[:10]:
            if sum(1 for c in l if c.isupper()) >= max(3, int(0.4*len(l))):
                seller = l.strip(" :,-")
                break

    # Description
    for i, l in enumerate(lines):
        if any(h in l.lower() for h in DESC_HINTS):
            # take same line after colon or next line
            after = l.split(":", 1)[-1].strip()
            if len(after) > 3:
                opis = after
            elif i + 1 < len(lines):
                opis = lines[i+1]
            break
    if not opis:
        # fallback to the longest mid-document line that's not numeric
        nonnum = [l for l in lines[5:50] if not re.fullmatch(r"[0-9\s\.,-]+", l)]
        if nonnum:
            opis = sorted(nonnum, key=len, reverse=True)[0][:200]

    return {"wystawca": seller, "opis": opis}

NUM = r"[0-9\s]+[.,][0-9]{2}"

def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_invoice(content_bytes: bytes, filename_hint: str = "") -> dict:
    # użyj istniejącej funkcji PDF -> tekst
    text = pdf_to_text(content_bytes)
    t = re.sub(r"[ \t]+", " ", text)
    lines = text.splitlines()

    # OPIS – szukamy pierwszej pozycji (wiersz zaczynający się od "1 ")
    m = re.search(r"(?m)^\s*1\s+(.+?)\s{2,}"+NUM+r"\s+"+NUM+r"\s+"+NUM+r"\s*$", text)
    if m:
        opis = m.group(1).strip()
    else:
        opis = None
        for i, ln in enumerate(lines):
            if re.search(r"\bLP\b.*Szczegóły", ln, flags=re.IGNORECASE):
                if i+1 < len(lines):
                    cand = re.split(r"\s{2,}"+NUM+r"(?:\s+"+NUM+r"){0,2}\s*$", lines[i+1])[0]
                    opis = cand.strip()
                break

    # TERMIN PŁATNOŚCI
    termin_platnosci = None
    m = re.search(r"Termin zapłaty:\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})", t, flags=re.IGNORECASE)
    if m:
        raw = m.group(1).replace(" ", "")
        for fmt in ("%d.%m.%Y","%d-%m-%Y","%d/%m/%Y","%d.%m.%y","%d-%m-%y","%d/%m/%y"):
            try:
                termin_platnosci = dt.datetime.strptime(raw, fmt).date().isoformat()
                break
            except Exception:
                pass

    # KWOTY (brutto preferencyjnie, w razie braku – netto)
    brutto = None
    m = re.search(r"(?:Wartość brutto:|Do zapłaty brutto:)\s*("+NUM+")", t, flags=re.IGNORECASE)
    if m:
        brutto = _to_float(m.group(1))

    netto = None
    m = re.search(r"Ogółem usługi\s*("+NUM+")", t, flags=re.IGNORECASE)
    if m:
        netto = _to_float(m.group(1))
    else:
        m = re.search(r"(?m)^\s*1\s+.+?\s{2,}("+NUM+")\s+"+NUM+r"\s+"+NUM+r"\s*$", text)
        if m:
            netto = _to_float(m.group(1))

    kwota = brutto or netto

    return {
        "opis": opis,
        "kwota": kwota,
        "netto": netto,
        "brutto": brutto,
        "termin_platnosci": termin_platnosci,
    }



def _sorted_index(df: pd.DataFrame, col: str, asc: bool) -> pd.Index:
    """Zwraca indeksy w kolejności sortowania po kolumnie `col`."""
    if col in {"kwota", "netto", "brutto"}:
        key = pd.to_numeric(df.get(col), errors="coerce")
    elif col in {"Data wprowadzenia rachunku", "termin_platnosci"}:
        key = pd.to_datetime(df.get(col), errors="coerce")
    else:
        key = df.get(col).astype(str)
    # mergesort = stabilne sortowanie (nie miesza rzędów przy equal)
    return key.sort_values(ascending=asc, kind="mergesort").index





def _excel_bytes_two_sheets(df_all: pd.DataFrame, df_due: pd.DataFrame, mask_amounts: bool) -> bytes:
    """Zwraca bytes pliku .xlsx z dwiema zakładkami: Wszystkie, Do_zaplaty.
       Jeśli mask_amounts=True (np. rola admin), kwoty są zamieniane na '—' także w pliku."""
    a = df_all.copy()
    d = df_due.copy()

    if mask_amounts:
        for col in ["kwota", "netto", "brutto"]:
            if col in a.columns:
                a[col] = "—"
            if col in d.columns:
                d[col] = "—"

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        a.to_excel(w, index=False, sheet_name="Wszystkie")
        d.to_excel(w, index=False, sheet_name="Do_zaplaty")
    buf.seek(0)
    return buf.getvalue()

def _excel_bytes_single(df: pd.DataFrame, sheet_name: str, mask_amounts: bool) -> bytes:
    """Pojedyncza tabela jako .xlsx (jedna zakładka)."""
    x = df.copy()
    if mask_amounts:
        for col in ["kwota", "netto", "brutto"]:
            if col in x.columns:
                x[col] = "—"
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        x.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.getvalue()


# Zmiana haseł i administrowanie użytkownikami

def _load_users_doc(service, folder_id) -> dict:
    """Wczytuje users.json z Drive. Jeśli brak – zwraca szablon."""
    f = find_file_by_name(service, folder_id, USERS_FILENAME)
    if not f:
        return {"version": 1, "users": []}
    raw = download_bytes(service, f["id"]) or b"{}"
    try:
        doc = json.loads(raw.decode("utf-8"))
        if "users" not in doc: doc["users"] = []
        if "version" not in doc: doc["version"] = 1
        return doc
    except Exception:
        return {"version": 1, "users": []}

def _save_users_doc(service, folder_id, doc: dict):
    payload = json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")
    upload_or_update_file(service, folder_id, USERS_FILENAME, payload, "application/json")

def _append_audit(service, folder_id, entry: dict):
    """Dopisuje rekord do audit logu (lista JSON)."""
    f = find_file_by_name(service, folder_id, USERS_AUDIT_FILENAME)
    items = []
    if f:
        raw = download_bytes(service, f["id"]) or b"[]"
        try:
            items = json.loads(raw.decode("utf-8"))
            if not isinstance(items, list): items = []
        except Exception:
            items = []
    items.append(entry)
    upload_or_update_file(
        service, folder_id, USERS_AUDIT_FILENAME,
        json.dumps(items, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json"
    )

def _user_find(doc: dict, email: str) -> dict | None:
    email = email.lower().strip()
    for u in doc.get("users", []):
        if u.get("email", "").lower() == email:
            return u
    return None

def _user_set_hash(doc: dict, email: str, role: str, pw_plain: str):
    """Ustawia/aktualizuje użytkownika i jego hash (tworzy jeśli brak)."""
    email = email.lower().strip()
    h = bcrypt.hashpw(pw_plain.encode(), bcrypt.gensalt()).decode()
    u = _user_find(doc, email)
    if u:
        u["hash"] = h
        u["role"] = role
    else:
        doc["users"].append({"email": email, "role": role, "hash": h})
    doc["version"] = int(doc.get("version", 1)) + 1

def verify_user_password(service, folder_id, email: str, pw_plain: str) -> str | None:
    """Zwraca rolę po poprawnym haśle, inaczej None."""
    doc = _load_users_doc(service, folder_id)
    u = _user_find(doc, email)
    if not u: return None
    try:
        if bcrypt.checkpw(pw_plain.encode(), u["hash"].encode()):
            return u.get("role")
    except Exception:
        return None
    return None

def change_own_password(service, folder_id, email: str, old_pw: str, new_pw: str) -> bool:
    doc = _load_users_doc(service, folder_id)
    u = _user_find(doc, email)
    if not u:
        st.error("Użytkownik nie istnieje.")
        return False
    if not bcrypt.checkpw(old_pw.encode(), u["hash"].encode()):
        st.error("Stare hasło niepoprawne.")
        return False
    _user_set_hash(doc, email, u.get("role",""), new_pw)
    _save_users_doc(service, folder_id, doc)
    _append_audit(service, folder_id, {
        "ts": int(time.time()), "who": email, "action": "change_password_self"
    })
    return True

def admin_set_password(service, folder_id, admin_email: str, target_email: str, role: str, new_pw: str) -> bool:
    doc = _load_users_doc(service, folder_id)
    _user_set_hash(doc, target_email, role, new_pw)
    _save_users_doc(service, folder_id, doc)
    _append_audit(service, folder_id, {
        "ts": int(time.time()), "who": admin_email, "action": "admin_set_password",
        "target": target_email, "role": role
    })
    return True


# ---------- APP ----------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    role = get_role_from_login()
    if not role:
        st.stop()

    service, folder_id = drive_service()

    # Load state and Excel
    state = load_state(service, folder_id)
    seen_ids = set(state.get("seen", []))
    df = load_excel_df(service, folder_id)

    # Uporządkuj kolejność kolumn (jeśli jakieś brakuje – zostaną na końcu)
    preferred = [
        "file_id", "nazwa_dokumentu", "Data wprowadzenia rachunku", "opis",
        "kwota", "netto", "brutto", "termin_platnosci", "zaplacone"
    ]
    df = df[[c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]]

    # Scan PDFs
    with st.spinner("Sprawdzam nowe pliki PDF w folderze..."):
        pdfs = list_pdfs(service, folder_id)
        new_rows = []
        for f in pdfs:
            fid = f["id"]
            if fid in seen_ids:
                continue
            # parse
            content = download_bytes(service, fid)
            parsed = parse_invoice(content, f.get("name", ""))
            row = {
                "file_id": fid,
                "nazwa_dokumentu": f.get("name", ""),  # nazwa pliku z Drive
                "Data wprowadzenia rachunku": f.get("modifiedTime", f.get("createdTime", ""))[:10],
                "opis": parsed.get("opis"),
                "kwota": parsed.get("kwota"),
                "netto": parsed.get("netto"),
                "brutto": parsed.get("brutto"),
                "termin_platnosci": parsed.get("termin_platnosci"),
                "zaplacone": False,
            }
            new_rows.append(row)
            seen_ids.add(fid)

        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
            # persist both state and excel
            save_excel_df(service, folder_id, df)
            save_state(service, folder_id, {"seen": list(seen_ids)})

    # --- Manual sync button ---
    colA, colB = st.columns([1, 3])
    with colA:
        if st.button("🔄 Odśwież / zsynchronizuj z Google Drive", width="stretch"):
            df, seen_ids, removed = sync_with_drive(service, folder_id, df, seen_ids)
            if removed:
                st.success(f"Usunięto {removed} pozycji, których już nie ma w folderze.")
            else:
                st.info("Brak zmian. Folder i tabela są zsynchronizowane.")

    # Role-based column visibility / editability
    view_df = df.copy()
    if role == "admin":
        # Admin nie widzi kwot
        for col in ["kwota", "netto", "brutto"]:
            if col in view_df.columns:
                view_df[col] = "—"

    st.subheader("Wszystkie dokumenty")

    # --- kontrolki sortowania (Wszystkie) ---
    sortable_all = [c for c in [
        "nazwa_dokumentu", "Data wprowadzenia rachunku", "opis",
        "kwota", "netto", "brutto", "termin_platnosci", "zaplacone"
    ] if c in df.columns]

    csa, csb = st.columns([2, 1])
    with csa:
        sort_col_all = st.selectbox(
            "Sortuj wg (wszystkie):",
            options=sortable_all,
            index=0,
            key="sort_all_col"
        )
    with csb:
        sort_dir_all = st.radio(
            "Kierunek",
            ["⬇︎ malejąco", "⬆︎ rosnąco"],
            horizontal=True,
            index=0,
            key="sort_all_dir"
        )

    asc_all = (sort_dir_all.endswith("rosnąco"))
    idx_all = _sorted_index(df, sort_col_all, asc_all)
    # sortujemy oba DF, żeby edycja i zapis szły w tej samej kolejności
    df = df.loc[idx_all].reset_index(drop=True)
    view_df = view_df.loc[idx_all].reset_index(drop=True)

    # --- przyciski pobierania (Wszystkie) ---
    mask_amounts = (role == "admin")
    fname_all = f"rachunki_wszystkie_{dt.datetime.now().strftime('%Y-%m-%d')}.xlsx"
    dl1, _ = st.columns([1, 3])
    with dl1:
        st.download_button(
            label="⬇️ Pobierz tę tabelę (Excel)",
            data=_excel_bytes_single(view_df, "Wszystkie", mask_amounts),
            file_name=fname_all,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_all_single",
        )

    # --- edytor (Wszystkie) ---
    can_edit_paid = (role == "ksiegowosc")
    disabled_param = [c for c in view_df.columns if c != "zaplacone"] if can_edit_paid else True

    edited = st.data_editor(
        view_df,
        num_rows="dynamic",
        disabled=disabled_param,
        column_config={
            "zaplacone": st.column_config.CheckboxColumn("Zapłacone?", help="Oznacz zapłatę"),
            "nazwa_dokumentu": st.column_config.TextColumn("NAZWA DOKUMENTU"),
            "Data wprowadzenia rachunku": st.column_config.TextColumn("Data wprowadzenia rachunku"),
        },
        hide_index=True,
        width="stretch",
        key="all_table",
    )

    # zapis zmian tylko dla księgowości
    if can_edit_paid and not df.empty and {"file_id","zaplacone"} <= set(df.columns) and {"file_id","zaplacone"} <= set(edited.columns):
        merged = df.merge(edited[["file_id","zaplacone"]], on="file_id", suffixes=("", "_new"), how="left")
        mask = merged["zaplacone"] != merged["zaplacone_new"]
        if mask.any():
            for fid in merged.loc[mask, "file_id"]:
                new_val = bool(merged.loc[merged["file_id"] == fid, "zaplacone_new"].iloc[0])
                df.loc[df["file_id"] == fid, "zaplacone"] = new_val
            save_excel_df(service, folder_id, df)
            st.success("Zapisano zmiany w Excelu")

    # --- Tab: Do zapłaty na dzisiaj ---
    st.subheader("Do zapłaty na dzisiaj")
    due_dates = pd.to_datetime(df["termin_platnosci"], errors="coerce").dt.date
    due_mask = (~df["zaplacone"].astype(bool)) & due_dates.notna() & (due_dates <= dt.date.today())
    due_df = df.loc[due_mask].copy()

    if role == "admin":
        for col in ["kwota", "netto", "brutto"]:
            if col in due_df.columns:
                due_df[col] = "—"

    # --- kontrolki sortowania (Do zapłaty) ---
    sortable_due = [c for c in [
        "termin_platnosci", "nazwa_dokumentu", "Data wprowadzenia rachunku",
        "opis", "kwota", "netto", "brutto"
    ] if c in due_df.columns]

    csd1, csd2 = st.columns([2, 1])
    with csd1:
        sort_col_due = st.selectbox(
            "Sortuj wg (do zapłaty):",
            options=sortable_due,
            index=0 if "termin_platnosci" in sortable_due else 0,
            key="sort_due_col"
        )
    with csd2:
        sort_dir_due = st.radio(
            "Kierunek",
            ["⬇︎ malejąco", "⬆︎ rosnąco"],
            horizontal=True,
            index=0,
            key="sort_due_dir"
        )

    asc_due = (sort_dir_due.endswith("rosnąco"))
    # sortujemy po bazowym df (niemaskowanym), ale wyświetlamy due_df (dla admina maskowane)
    base_for_sort = df.loc[due_mask].copy()
    idx_due = _sorted_index(base_for_sort, sort_col_due, asc_due)
    due_df = due_df.iloc[base_for_sort.index.get_indexer(idx_due)].reset_index(drop=True)

    # --- przyciski pobierania (Do zapłaty + łączny plik) ---
    fname_due = f"rachunki_do_zaplaty_{dt.datetime.now().strftime('%Y-%m-%d')}.xlsx"
    col_d1, col_d2 = st.columns([1, 1])
    with col_d1:
        st.download_button(
            label="⬇️ Pobierz tę tabelę (Excel)",
            data=_excel_bytes_single(due_df, "Do_zaplaty", mask_amounts=(role == "admin")),
            file_name=fname_due,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_due_single",
        )
    with col_d2:
        both_bytes = _excel_bytes_two_sheets(
            df_all=view_df,   # szanujemy aktualny widok użytkownika
            df_due=due_df,
            mask_amounts=(role == "admin")
        )
        st.download_button(
            label="⬇️ Pobierz obie tabele (Excel: 2 zakładki)",
            data=both_bytes,
            file_name=f"rachunki_{dt.datetime.now().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_both_two_sheets",
        )

    # --- render tabeli 'Do zapłaty' ---
    st.dataframe(due_df, width="stretch", hide_index=True)

    # Diagnostyka tylko dla admina
    if role == "admin":
        with st.expander("🛠️ Diagnostyka / pomoc"):
            st.write({
                "logged_role": role,
                "folder_id": folder_id,
                "OCR_AVAILABLE": OCR_AVAILABLE,
                "rows_total": len(df),
            })
            st.caption("Jeśli OCR nie działa, zainstaluj Poppler i Tesseract na serwerze.")
st.divider()
st.subheader("🔐 Zmiana hasła")

email_logged = st.session_state.get("user_email") or ""
col1, col2 = st.columns(2)

with col1:
    st.caption("Zmiana własnego hasła")
    old_pw = st.text_input("Stare hasło", type="password", key="own_old")
    new_pw1 = st.text_input("Nowe hasło", type="password", key="own_new1")
    new_pw2 = st.text_input("Powtórz nowe hasło", type="password", key="own_new2")
    if st.button("Zmień własne hasło", type="primary", key="btn_change_own"):
        if new_pw1 != new_pw2:
            st.error("Nowe hasła nie są identyczne.")
        elif len(new_pw1) < 8:
            st.error("Hasło musi mieć co najmniej 8 znaków.")
        else:
            ok = change_own_password(service, folder_id, email_logged, old_pw, new_pw1)
            if ok:
                st.success("Hasło zmienione.")
                st.experimental_rerun()

with col2:
    if role == "admin":
        st.caption("Reset / ustawienie hasła przez administratora")
        target_email = st.text_input("E-mail użytkownika", value=email_logged, key="adm_target")
        role_sel = st.selectbox("Rola", ["ksiegowosc", "krzysztof", "admin"], key="adm_role")
        new_pw_admin = st.text_input("Nowe hasło użytkownika", type="password", key="adm_new")
        if st.button("Ustaw hasło użytkownika", key="btn_admin_set"):
            if len(new_pw_admin) < 8:
                st.error("Hasło musi mieć co najmniej 8 znaków.")
            else:
                admin_set_password(service, folder_id, email_logged, target_email, role_sel, new_pw_admin)
                st.success(f"Ustawiono hasło dla: {target_email}")


if __name__ == "__main__":
    main()
