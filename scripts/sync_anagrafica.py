import requests
import json
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================

BMAN_BASE_URL = "https://DOMINIO.bman.it:3555/bmanapi.asmx"
BMAN_KEY = "INSERISCI_CHIAVE_BMAN"

SPREADSHEET_ID = "INSERISCI_ID_SHEET"
SHEET_NAME = "ARTICOLI"

SERVICE_ACCOUNT_FILE = "service_account.json"

DRY_RUN = True  # <<< METTI False SOLO DOPO I TEST

# campi consentiti (Google Sheet → BMAN)
UPDATABLE_FIELDS = {
    "Brand": "opzionale1",
    "Titolo IT": "opzionale2",
    "Titolo FR": "opzionale3",
    "Titolo EN": "opzionale4",
    "Titolo ES": "opzionale5",
    "Titolo DE": "opzionale6",
    "Descrizione IT": "opzionale13",
    "Descrizione FR": "opzionale14",
    "Descrizione EN": "opzionale15",
    "Descrizione ES": "opzionale16",
    "Descrizione DE": "opzionale17",
    "Categoria1": "strCategoria1",
    "Categoria2": "strCategoria2"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

# =========================
# GOOGLE SHEETS
# =========================

def get_sheets():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

# =========================
# BMAN API
# =========================

def bman_get_article_by_id(article_id):
    payload = {
        "chiave": BMAN_KEY,
        "filtri": json.dumps([
            {"chiave": "ID", "operatore": "=", "valore": str(article_id)}
        ]),
        "ordinamentoCampo": "ID",
        "ordinamentoDirezione": 1,
        "numeroPagina": 1,
        "listaDepositi": "",
        "dettaglioVarianti": False
    }

    r = requests.post(f"{BMAN_BASE_URL}/getAnagrafiche", json=payload, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None


def bman_update_article(payload):
    payload["chiave"] = BMAN_KEY
    r = requests.post(f"{BMAN_BASE_URL}/InsertAnagrafica", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# =========================
# UTILS
# =========================

def norm(v):
    return "" if v is None else str(v).strip()

# =========================
# CORE
# =========================

def sync_articoli():
    sheets = get_sheets()
    result = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_NAME
    ).execute()

    rows = result.get("values", [])
    headers = rows[0]
    data = rows[1:]

    idx = {h: i for i, h in enumerate(headers)}

    for row in data:
        try:
            if norm(row[idx["Script"]]).lower() != "si":
                continue

            article_id = norm(row[idx["ID"]])
            codice = norm(row[idx["Codice"]])

            if not article_id and not codice:
                continue

            bman_article = bman_get_article_by_id(article_id)
            if not bman_article:
                logging.warning(f"Articolo non trovato ID={article_id}")
                continue

            diff = {}
            for sheet_col, bman_field in UPDATABLE_FIELDS.items():
                new_val = norm(row[idx.get(sheet_col)])
                old_val = norm(bman_article.get(bman_field))
                if new_val != old_val:
                    diff[bman_field] = new_val

            if not diff:
                logging.info(f"[SKIP] {codice} nessuna modifica")
                continue

            payload = {
                "IDAnagrafica": article_id,
                "codice": codice,
                **diff
            }

            if DRY_RUN:
                logging.info(f"[DRY-RUN] {codice} → {diff}")
            else:
                bman_update_article(payload)
                logging.info(f"[UPDATE] {codice} aggiornato")

        except Exception as e:
            logging.error(f"Errore riga {row}: {e}")

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    sync_articoli()
