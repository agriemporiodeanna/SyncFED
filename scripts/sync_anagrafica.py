import requests
import json
import logging
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# CONFIGURAZIONE
# =========================

# --- BMAN ---
BMAN_BASE_URL = "https://DOMINIO.bman.it:3555/bmanapi.asmx"
BMAN_KEY = "INSERISCI_CHIAVE_BMAN"

# --- GOOGLE SHEET ---
SPREADSHEET_ID = "INSERISCI_ID_GOOGLE_SHEET"
SHEET_NAME = "ARTICOLI"
SERVICE_ACCOUNT_FILE = "service_account.json"

# --- MODALITÀ ---
DRY_RUN = True        # <<< METTI False SOLO DOPO I TEST
REQUEST_DELAY = 0.25 # per rispettare limite 5 req/sec BMAN

# --- MAPPATURA CAMPI (Sheet → BMAN) ---
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

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================
# GOOGLE SHEET
# =========================

def get_worksheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE,
        scope
    )

    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# =========================
# BMAN API
# =========================

def bman_get_article(article_id):
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

    r = requests.post(
        f"{BMAN_BASE_URL}/getAnagrafiche",
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    data = r.json()
    return data[0] if data else None


def bman_update_article(payload):
    payload["chiave"] = BMAN_KEY

    r = requests.post(
        f"{BMAN_BASE_URL}/InsertAnagrafica",
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    return r.json()

# =========================
# UTILITY
# =========================

def norm(val):
    if val is None:
        return ""
    return str(val).strip()

# =========================
# CORE SYNC
# =========================

def sync_articoli():
    ws = get_worksheet()
    rows = ws.get_all_values()

    if not rows or len(rows) < 2:
        logging.warning("Foglio vuoto o senza dati")
        return

    headers = rows[0]
    data_rows = rows[1:]
    idx = {h: i for i, h in enumerate(headers)}

    updated = 0
    skipped = 0

    for row in data_rows:
        try:
            if norm(row[idx["Script"]]).lower() != "si":
                skipped += 1
                continue

            article_id = norm(row[idx["ID"]])
            codice = norm(row[idx["Codice"]])

            if not article_id:
                skipped += 1
                continue

            bman_data = bman_get_article(article_id)
            time.sleep(REQUEST_DELAY)

            if not bman_data:
                logging.warning(f"Articolo ID {article_id} non trovato su BMAN")
                continue

            diff = {}
            for sheet_col, bman_field in UPDATABLE_FIELDS.items():
                new_val = norm(row[idx.get(sheet_col)])
                old_val = norm(bman_data.get(bman_field))
                if new_val != old_val:
                    diff[bman_field] = new_val

            if not diff:
                logging.info(f"[SKIP] {codice} nessuna variazione")
                continue

            payload = {
                "IDAnagrafica": int(article_id),
                "codice": codice,
                **diff
            }

            if DRY_RUN:
                logging.info(f"[DRY-RUN] {codice} → {diff}")
            else:
                bman_update_article(payload)
                logging.info(f"[UPDATE] {codice} aggiornato")
                updated += 1
                time.sleep(REQUEST_DELAY)

        except Exception as e:
            logging.error(f"Errore su riga {row}: {e}")

    logging.info(
        f"SYNC COMPLETATO | aggiornati={updated} | skippati={skipped}"
    )

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    sync_articoli()
