import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Setup Credenziali
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # Mappatura Campi Articoli (Solo Anagrafica Testuale)
    mappatura = {
        "ID": "ID", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale11": "Script", "opzionale12": "Descrizione IT"
    }

    workbook = client.open_by_key(sheet_id)
    # Usiamo il tab principale degli articoli
    sheet = workbook.get_worksheet(0)

    # 2. Scarica dati correnti da Bman per confronto
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_body_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
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
    
    resp = requests.post(bman_url, data=soap_body_get, headers={'Content-Type': 'text/xml'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    bman_items = {str(i['ID']): i for i in json.loads(result_text)} if result_text else {}

    # 3. Leggi il Foglio Google
    sheet_rows = sheet.get_all_records()
    formats = []
    aggiornati = 0

    # 4. Confronto e Sincronizzazione verso Bman
    for row_idx, row in enumerate(sheet_rows, start=2):
        art_id = str(row.get("ID"))
        if not art_id or art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        modificato = False
        nuovi_dati = item_bman.copy()

        for col_idx, (bman_key_attr, header) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(header, "")).strip()
            val_bman = str(item_bman.get(bman_key_attr, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)

            if not val_sheet:
                # Rosso se vuoto
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
            elif val_sheet != val_bman:
                # Se c'è un dato nuovo nel foglio, prepara l'invio a Bman
                nuovi_dati[bman_key_attr] = val_sheet
                modificato = True
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
            else:
                # Bianco se sincronizzato
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})

        if modificato:
            # Invio a Bman con setAnagrafica
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(nuovi_dati)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            requests.post(bman_url, data=soap_body_set, headers={'Content-Type': 'text/xml'}, timeout=30)
            aggiornati += 1

    if formats:
        sheet.batch_format(formats)
    
    return f"Sincronizzazione Articoli completata. Aggiornati {aggiornati} record su Bman."
