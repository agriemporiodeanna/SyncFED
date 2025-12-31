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
    
    # Mappatura Campi (Coerente con i tuoi test precedenti)
    mappatura = {
        "ID": "ID Contatto",
        "codice": "Codice",
        "opzionale1": "Brand",
        "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR",
        "opzionale12": "Descrizione IT"
    }

    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(1) # Tab Anagrafica

    # 2. Scarica dati da Bman (Stato ATTUALE sul server)
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

    # 3. Leggi Google Sheet (Cosa hai scritto TU)
    sheet_values = sheet.get_all_records()
    
    formats = []
    contatore_ok = 0
    log_dettagli = []

    for row_idx, row in enumerate(sheet_values, start=2):
        c_id = str(row.get("ID Contatto")).strip()
        if not c_id or c_id not in bman_data:
            continue
        
        art_bman = bman_data[c_id]
        nuovo_payload = art_bman.copy()
        riga_modificata = False

        for col_idx, (bman_key_attr, sheet_header) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(sheet_header, "")).strip()
            val_bman = str(art_bman.get(bman_key_attr, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)

            # DEBUG: Logghiamo solo se c'è una differenza potenziale
            if val_sheet and val_sheet != val_bman:
                log_dettagli.append(f"🔍 Rilevata modifica ID {c_id}: '{val_bman}' -> '{val_sheet}'")
                nuovo_payload[bman_key_attr] = val_sheet
                riga_modificata = True
                # Prepariamo la cella per tornare bianca
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}})
            elif not val_sheet:
                # Se ancora vuota, resta rossa
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}})

        if riga_modificata:
            # 4. INVIO A BMAN (setAnagrafica)
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
            
            try:
                tree_set = ET.fromstring(res_set.content)
                res_val = tree_set.find('.//{http://cloud.bman.it/}setAnagraficaResult').text
                
                if res_val == "1":
                    contatore_ok += 1
                    log_dettagli.append(f"✅ ID {c_id} aggiornato con successo.")
                else:
                    log_dettagli.append(f"❌ Errore Bman ID {c_id}: Risposta {res_val}")
            except Exception as e:
                log_dettagli.append(f"❌ Errore parsing risposta per ID {c_id}: {str(e)}")

    # Applica colori
    if formats:
        sheet.batch_format(formats)
    
    if not log_dettagli:
        return "Nessuna modifica rilevata tra il foglio e Bman."

    return f"Sincronizzazione completata.\nAggiornati: {contatore_ok}\n\nDETTAGLIO LOG:\n" + "\n".join(log_dettagli)
