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


def _google_client_from_env():
    """
    OPZIONE A: gspread + oauth2client con credenziali da variabili d'ambiente.
    Richiede:
      - GOOGLE_CLIENT_EMAIL
      - GOOGLE_PRIVATE_KEY  (con \\n oppure \n)
      - GOOGLE_SHEET_ID
    """
    client_email = _env("GOOGLE_CLIENT_EMAIL")
    private_key = _env("GOOGLE_PRIVATE_KEY")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Alcune versioni di oauth2client vogliono campi "extra" presenti
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


def _bman_get_anagrafiche(bman_url: str, bman_key: str, filtri: list, page: int = 1):
    """
    getAnagrafiche via SOAP (come nel tuo progetto).
    """
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
      <numeroPagina>{page}</numeroPagina>
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
        return []
    return json.loads(node.text)


def run():
    """
    Pulsante 3:
      - legge articoli Bman con opzionale11 = 'si'
      - scrive su Google Sheet (worksheet 0)
      - colonne: quelle concordate nel progetto
    """
    bman_key = _env("BMAN_API_KEY")
    bman_url = _env("BMAN_BASE_URL")  # es: https://emporiodeanna.bman.it/bmanapi.asmx
    sheet_id = _env("GOOGLE_SHEET_ID")

    client = _google_client_from_env()
    sheet = client.open_by_key(sheet_id).get_worksheet(0)

    # filtro: Script = si (campo opzionale11)
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]

    articoli = _bman_get_anagrafiche(bman_url, bman_key, filtri, page=1)

    data_to_write = []
    for art in articoli:
        iva = float(art.get("iva", 0) or 0)
        przc_netto = float(art.get("przc", 0) or 0)
        prezzo_lordo = round(przc_netto * (1 + iva / 100), 2)
        prezzo_minimo = round(prezzo_lordo * 0.8, 2)

        row = [
            art.get("ID", ""),
            art.get("codice", ""),
            art.get("opzionale1", ""),     # Brand
            art.get("opzionale2", ""),     # Titolo IT
            art.get("opzionale5", ""),     # Vinted
            art.get("opzionale6", ""),     # Titolo FR
            art.get("opzionale7", ""),     # Titolo EN
            art.get("opzionale8", ""),     # Titolo ES
            art.get("opzionale9", ""),     # Titolo DE
            art.get("opzionale11", ""),    # Script
            art.get("opzionale12", ""),    # Descrizione IT
            art.get("opzionale13", ""),    # Descrizione FR
            art.get("opzionale14", ""),    # Descrizione EN
            art.get("opzionale15", ""),    # Descrizione ES
            art.get("opzionale16", ""),    # Descrizione DE
            prezzo_minimo,
            prezzo_lordo,
            art.get("iva", ""),
            art.get("categoria1str", ""),
            art.get("categoria2str", ""),
        ]
        data_to_write.append(row)

    header = [
        "ID", "Codice", "Brand", "Titolo IT", "Vinted",
        "Titolo FR", "Titolo EN", "Titolo ES", "Titolo DE",
        "Script",
        "Descrizione IT", "Descrizione FR", "Descrizione EN", "Descrizione ES", "Descrizione DE",
        "Prezzo Minimo", "Prezzo", "Iva",
        "Categoria1", "Categoria2",
    ]

    sheet.clear()
    sheet.update("A1", [header] + data_to_write)

    return {
        "status": "ok",
        "message": f"Export completato: {len(articoli)} articoli scritti su Google Sheet.",
        "count": len(articoli),
    }

