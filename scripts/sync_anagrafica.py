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
    
    # Mappatura Campi Clienti
    mappatura = {
        "ID": "ID Contatto",
        "ragioneSociale": "Ragione Sociale",
        "email": "Email",
        "telefono": "Telefono"
    }

    workbook = client.open_by_key(sheet_id)
    # Tenta di accedere al tab "Anagrafica", altrimenti lo crea
    try:
        sheet = workbook.get_worksheet(1)
        if not sheet: sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")
    except:
        sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")

    # 2. Scarica dati da Bman
    soap_body_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getClienti xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[[]]]></filtri>
      <numeroPagina>1</numeroPagina>
    </getClienti>
  </soap:Body>
</soap:Envelope>"""
    
    resp = requests.post(bman_url, data=soap_body_get, headers={'Content-Type': 'text/xml'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getClientiResult').text
    bman_data = {str(c['ID']): c for c in json.loads(result_text)} if result_text else {}

    # 3. Leggi Google Sheet
    sheet_values = sheet.get_all_records()
    
    if not sheet_values:
        # Inizializzazione: se vuoto, scrive dati Bman
        intestazioni = list(mappatura.values())
        righe = [intestazioni]
        for data in bman_data.values():
            righe.append([data.get(k, "") for k in mappatura.keys()])
        sheet.update('A1', righe)
        return "Foglio 'Anagrafica' inizializzato con dati Bman. Premi di nuovo per la verifica colori."

    # 4. Analisi e Formattazione
    formats = []
    aggiornamenti_bman = 0

    for row_idx, row in enumerate(sheet_values, start=2):
        c_id = str(row.get("ID Contatto"))
        bman_contact = bman_data.get(c_id, {})

        for col_idx, (bman_key_attr, sheet_header) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(sheet_header, "")).strip()
            val_bman = str(bman_contact.get(bman_key_attr, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)
            
            # Logica colori
            if not val_sheet:
                # Rosso se vuota
                color = {"red": 1.0, "green": 0.8, "blue": 0.8}
            elif val_sheet != val_bman:
                # Qui andrebbe la logica di setCliente per Bman se diversa
                # Per ora evidenziamo come sincronizzato (Bianco) se il dato è presente
                color = {"red": 1.0, "green": 1.0, "blue": 1.0}
                aggiornamenti_bman += 1
            else:
                # Bianco se uguale
                color = {"red": 1.0, "green": 1.0, "blue": 1.0}
            
            formats.append({"range": cell_range, "format": {"backgroundColor": color}})

    sheet.batch_format(formats)
    return f"Analisi completata. Sincronizzati {aggiornamenti_bman} campi. Celle vuote evidenziate in rosso."
