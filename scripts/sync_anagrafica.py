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
    
    # Mappatura Campi Articoli (Anagrafica Prodotti)
    mappatura = {
        "ID": "ID Contatto",
        "codice": "Codice",
        "opzionale1": "Brand",
        "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR",
        "opzionale12": "Descrizione IT"
    }

    workbook = client.open_by_key(sheet_id)
    try:
        sheet = workbook.get_worksheet(1)
        if not sheet: sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")
    except:
        sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")

    # 2. Scarica dati aggiornati usando getAnagrafiche (come da tua documentazione)
    # Usiamo il filtro opzionale11='si' come nei test precedenti
    filtri_bman = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    
    soap_body_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri_bman)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""
    
    resp = requests.post(bman_url, data=soap_body_get, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    bman_data = {str(c['ID']): c for c in json.loads(result_text)} if result_text else {}

    # 3. Analisi Google Sheet
    sheet_values = sheet.get_all_records()
    
    if not sheet_values:
        # Inizializzazione
        intestazioni = list(mappatura.values())
        righe = [intestazioni]
        for data in bman_data.values():
            righe.append([data.get(k, "") for k in mappatura.keys()])
        sheet.update('A1', righe)
        return "Foglio inizializzato. Modifica i dati e ripremi il pulsante."

    # 4. Sincronizzazione Bidirezionale con setAnagrafica
    formats = []
    contatore_ok = 0
    log_dettagli = []

    for row_idx, row in enumerate(sheet_values, start=2):
        c_id = str(row.get("ID Contatto"))
        if c_id not in bman_data: continue
        
        art_originale = bman_data[c_id]
        nuovo_payload = art_originale.copy()
        modificato = False

        for col_idx, (bman_key_attr, sheet_header) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(sheet_header, "")).strip()
            val_bman = str(art_originale.get(bman_key_attr, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)

            if val_sheet and val_sheet != val_bman:
                nuovo_payload[bman_key_attr] = val_sheet
                modificato = True
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}})
                log_dettagli.append(f"ID {c_id}: {sheet_header} aggiornato.")
            elif not val_sheet:
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}})

        if modificato:
            # USIAMO setAnagrafica per i prodotti
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(nuovo_payload)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            headers_set = {'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}
            res_set = requests.post(bman_url, data=soap_body_set, headers=headers_set, timeout=30)
            tree_set = ET.fromstring(res_set.content)
            res_val = tree_set.find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            
            if res_val == "1":
                contatore_ok += 1
            else:
                log_dettagli.append(f"❌ Errore ID {c_id}: Risposta {res_val}")

    if formats:
        sheet.batch_format(formats)
    
    return f"Sincronizzazione completata. Aggiornati: {contatore_ok}\n" + "\n".join(log_dettagli)
