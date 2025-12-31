import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Setup Credenziali e Connessione
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "bman-sync"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"), 
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", "12345"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    
    # Selezione foglio "Anagrafica" (Tab 2)
    try:
        sheet = workbook.get_worksheet(1)
        if not sheet: sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")
    except:
        sheet = workbook.add_worksheet(title="Anagrafica", rows="100", cols="10")

    # 2. Controllo e Ripristino Intestazioni
    mappatura = {
        "ID": "ID Contatto", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR", "opzionale12": "Descrizione IT"
    }
    
    raw_data = sheet.get_all_values()
    if not raw_data or not raw_data[0]:
        sheet.update('A1', [list(mappatura.values())])
        return "Intestazioni create nel foglio 'Anagrafica'. Inserisci i dati e riprova."

    headers = [h.strip().upper() for h in raw_data[0]]
    idx_id = -1
    if "ID CONTATTO" in headers:
        idx_id = headers.index("ID CONTATTO")
    else:
        # Forza la riga 1 se le intestazioni sono sparite
        sheet.update('A1', [list(mappatura.values())])
        return "ID Contatto non trovato. Ho ripristinato le intestazioni. Controlla il foglio."

    # 3. Ciclo di Sincronizzazione (Update -> Insert)
    log_finale = []
    formats = []

    for r_idx, row in enumerate(raw_data[1:], start=2):
        item_id = str(row[idx_id]).strip()
        if not item_id: continue

        # Costruzione Payload con Campi Obbligatori per evitare errore -23
        payload = {
            "ID": item_id,
            "IDDeposito": 1,
            "tipoArt": 0, # Articolo semplice
            "iva": "22",  # Valore di esempio, bMan lo richiede spesso
            "gestioneMagazzino": True
        }

        # Popolamento dai dati del foglio
        for bman_key, sheet_title in mappatura.items():
            try:
                col_idx = headers.index(sheet_title.upper())
                val = str(row[col_idx]).strip()
                payload[bman_key] = val
                
                # Colore cella
                cell_ref = gspread.utils.rowcol_to_a1(r_idx, col_idx + 1)
                color = {"red": 1, "green": 1, "blue": 1} if val else {"red": 1, "green": 0.8, "blue": 0.8}
                formats.append({"range": cell_ref, "format": {"backgroundColor": color}})
            except: continue

        # --- LOGICA DI INVIO ---
        successo = False
        metodo = ""

        # Tentativo 1: setAnagrafica
        soap_set = f'<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><setAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></setAnagrafica></soap:Body></soap:Envelope>'
        
        try:
            r = requests.post(bman_url, data=soap_set, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}, timeout=20)
            res = ET.fromstring(r.content).find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            if res == "1":
                successo = True
                metodo = "Aggiornato"
        except: pass

        # Tentativo 2: InsertAnagrafica
        if not successo:
            soap_ins = f'<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><InsertAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></InsertAnagrafica></soap:Body></soap:Envelope>'
            try:
                r_ins = requests.post(bman_url, data=soap_ins, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=20)
                res_ins = ET.fromstring(r_ins.content).find('.//{http://cloud.bman.it/}InsertAnagraficaResult').text
                if int(res_ins) > 0:
                    metodo = f"Creato (ID {res_ins})"
                else:
                    metodo = f"Errore ({res_ins})"
            except: metodo = "Errore Connessione"

        log_finale.append(f"ID {item_id}: {metodo}")

    if formats:
        sheet.batch_format(formats)

    return "Esito:\n" + "\n".join(log_finale)
