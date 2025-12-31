import os
import requests
import json
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Setup Credenziali (FIX KeyError: 'private_key_id')
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
    
    # 2. Selezione Foglio (Proviamo a essere flessibili)
    try:
        sheet = workbook.get_worksheet(1) # Tab 2
        if not sheet: sheet = workbook.get_worksheet(0)
    except:
        sheet = workbook.get_worksheet(0)

    raw_data = sheet.get_all_values()
    if len(raw_data) < 1: return "Errore: Il foglio selezionato è completamente vuoto."

    # Normalizziamo le intestazioni per trovarle anche se scritte male
    headers = [h.strip().upper() for h in raw_data[0]]
    
    def find_col(name):
        try: return headers.index(name.upper())
        except: return -1

    idx_id = find_col("ID Contatto")
    if idx_id == -1: 
        return f"Errore: Colonna 'ID Contatto' non trovata. Colonne viste: {headers}"

    # 3. Mappatura Campi per Payload
    mappatura = {
        "ID": "ID Contatto", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR", "opzionale12": "Descrizione IT"
    }

    log_finale = []
    formats = []

    # 4. Ciclo di Sincronizzazione
    for r_idx, row in enumerate(raw_data[1:], start=2):
        item_id = str(row[idx_id]).strip()
        if not item_id: continue

        payload = {"IDDeposito": 1, "tipoArt": 0}
        for bman_key_attr, sheet_title in mappatura.items():
            c_idx = find_col(sheet_title)
            if c_idx != -1:
                val = str(row[c_idx]).strip()
                payload[bman_key_attr] = val
                
                # Formattazione: Rosso se cella vuota, Bianco se piena
                cell_ref = gspread.utils.rowcol_to_a1(r_idx, c_idx + 1)
                color = {"red": 1, "green": 1, "blue": 1} if val else {"red": 1, "green": 0.8, "blue": 0.8}
                formats.append({"range": cell_ref, "format": {"backgroundColor": color}})

        # --- LOGICA DOPPIO TENTATIVO ---
        successo = False
        # Tenta UPDATE
        soap_set = f'<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><setAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></setAnagrafica></soap:Body></soap:Envelope>'
        
        try:
            r = requests.post(bman_url, data=soap_set, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}, timeout=20)
            res = ET.fromstring(r.content).find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            if res == "1":
                successo = True
                log_finale.append(f"ID {item_id}: Aggiornato")
        except: pass

        if not successo:
            # Tenta INSERT
            soap_ins = f'<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><InsertAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></InsertAnagrafica></soap:Body></soap:Envelope>'
            try:
                r = requests.post(bman_url, data=soap_ins, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=20)
                res_ins = ET.fromstring(r.content).find('.//{http://cloud.bman.it/}InsertAnagraficaResult').text
                if int(res_ins) > 0:
                    log_finale.append(f"ID {item_id}: Creato nuovo (ID {res_ins})")
                else:
                    log_finale.append(f"ID {item_id}: Errore ({res_ins})")
            except:
                log_finale.append(f"ID {item_id}: Errore connessione")

    if formats:
        sheet.batch_format(formats)

    return "Esito:\n" + "\n".join(log_finale)
