import os
import json
import time
import requests
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials


def _env(name: str, required: bool = True, default: str | None = None) -> str | None:
    v = os.environ.get(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise Exception(f"Manca variabile d'ambiente: {name}")
    return v


def _to_bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "si", "on")


def _norm(v):
    if v is None:
        return ""
    return str(v).strip()


def _to_float(v, default=0.0):
    try:
        s = str(v).replace(",", ".").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _google_client_from_env():
    client_email = _env("GOOGLE_CLIENT_EMAIL")
    private_key = _env("GOOGLE_PRIVATE_KEY")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "syncfed-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "0000000000"),
        "private_key": private_key.replace("\\n", "\n"),
        "client_email": client_email,
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", "0000000000"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}",
    }

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def _bman_get_anagrafica_by_id(bman_url: str, bman_key: str, article_id: str):
    filtri = [{"chiave": "ID", "operatore": "=", "valore": str(article_id)}]

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://cloud.bman.it/getAnagrafiche",
    }

    resp = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    node = root.find(".//{http://cloud.bman.it/}getAnagraficheResult")
    if node is None or node.text is None or node.text.strip() == "":
        return None

    arr = json.loads(node.text)
    if not arr:
        return None
    return arr[0]


def _bman_insert_anagrafica_json(bman_url: str, payload: dict):
    """
    ASMX JSON endpoint: POST {bman_url}/InsertAnagrafica
    headers application/json
    risposta spesso in forma {"d": ...}
    """
    url = bman_url.rstrip("/") + "/InsertAnagrafica"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()

    try:
        j = resp.json()
        return j.get("d", j)
    except Exception:
        return resp.text


def run():
    """
    Pulsante 4:
      - legge Google Sheet (worksheet 0)
      - aggiorna Bman solo per righe Script=si
      - confronto: campi esportati dal pulsante 3
    """
    bman_key = _env("BMAN_API_KEY")
    bman_url = _env("BMAN_BASE_URL")
    sheet_id = _env("GOOGLE_SHEET_ID")

    dry_run = _to_bool(os.environ.get("SYNC_DRY_RUN"), default=True)
    request_delay = float(os.environ.get("REQUEST_DELAY", "0.25"))  # ~4 req/sec

    # Mapping colonne Sheet -> campi Bman
    # (coerente con sync_articoli.py)
    UPDATABLE = {
        "Brand": "opzionale1",
        "Titolo IT": "opzionale2",
        "Vinted": "opzionale5",
        "Titolo FR": "opzionale6",
        "Titolo EN": "opzionale7",
        "Titolo ES": "opzionale8",
        "Titolo DE": "opzionale9",
        "Descrizione IT": "opzionale12",
        "Descrizione FR": "opzionale13",
        "Descrizione EN": "opzionale14",
        "Descrizione ES": "opzionale15",
        "Descrizione DE": "opzionale16",
        "Categoria1": "strCategoria1",
        "Categoria2": "strCategoria2",
        # Prezzo/Iva gestiti a parte con conversione
    }

    client = _google_client_from_env()
    ws = client.open_by_key(sheet_id).get_worksheet(0)

    rows = ws.get_all_values()
    if len(rows) < 2:
        return {"status": "ok", "message": "Google Sheet vuoto: nulla da sincronizzare.", "updated": 0, "skipped": 0}

    headers = rows[0]
    idx = {h.strip(): i for i, h in enumerate(headers)}

    required_cols = ["ID", "Codice", "Script"]
    for c in required_cols:
        if c not in idx:
            raise Exception(f"Colonna mancante nel Google Sheet: {c}")

    updated = 0
    skipped = 0
    errors = 0

    for r in rows[1:]:
        try:
            script_val = _norm(r[idx["Script"]]).lower()
            if script_val != "si":
                skipped += 1
                continue

            article_id = _norm(r[idx["ID"]])
            codice = _norm(r[idx["Codice"]])

            if article_id == "" or codice == "":
                skipped += 1
                continue

            bman = _bman_get_anagrafica_by_id(bman_url, bman_key, article_id)
            time.sleep(request_delay)

            if not bman:
                errors += 1
                continue

            diff = {}

            # Campi testo/categorie
            for sheet_col, bman_field in UPDATABLE.items():
                if sheet_col not in idx:
                    continue
                new_val = _norm(r[idx[sheet_col]])
                old_val = _norm(bman.get(bman_field))
                if new_val != old_val:
                    diff[bman_field] = new_val

            # Prezzo + IVA:
            # In sheet: "Prezzo" è LORDO; in Bman: przc è NETTO.
            iva_sheet = _to_float(r[idx["Iva"]], default=_to_float(bman.get("iva", 0), 0))
            prezzo_lordo_sheet = _to_float(r[idx["Prezzo"]], default=None) if "Prezzo" in idx else None

            if iva_sheet is not None:
                old_iva = _to_float(bman.get("iva", 0), 0)
                if abs(iva_sheet - old_iva) > 1e-9:
                    diff["iva"] = iva_sheet

            if prezzo_lordo_sheet is not None:
                # converti in netto
                denom = (1.0 + (iva_sheet or 0.0) / 100.0)
                przc_new = prezzo_lordo_sheet / denom if denom != 0 else prezzo_lordo_sheet
                przc_new = round(przc_new, 4)

                old_przc = _to_float(bman.get("przc", 0), 0.0)
                if abs(przc_new - old_przc) > 1e-9:
                    diff["przc"] = przc_new

            if not diff:
                skipped += 1
                continue

            # Payload minimo: chiave + IDDeposito + codice + campi diff.
            # Nota: usiamo InsertAnagrafica in modalità "update" (come già impostato nel tuo progetto).
            payload = {
                "chiave": bman_key,
                "IDDeposito": int(os.environ.get("BMAN_ID_DEPOSITO", "1")),
                "codice": codice,
                "IDAnagrafica": int(article_id),
                **diff
            }

            if dry_run:
                # Non scrive su Bman
                # (metti SYNC_DRY_RUN=false su Render quando vuoi andare LIVE)
                pass
            else:
                _bman_insert_anagrafica_json(bman_url, payload)
                time.sleep(request_delay)
                updated += 1

        except Exception:
            errors += 1

    return {
        "status": "ok",
        "message": (
            "Sync anagrafica completata "
            + ("(DRY_RUN attivo: nessuna scrittura su Bman)." if dry_run else "(LIVE: aggiornamenti applicati su Bman).")
        ),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "dry_run": dry_run,
    }
