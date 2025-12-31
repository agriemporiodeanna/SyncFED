import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # 2. Configurazione Google (FIX KeyError: 'private_key_id')
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Recuperiamo o generiamo ID fittizi se mancano, ma la chiave deve ESISTERE
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "bman-sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "a1b2c3d4e5f6g7h8i9j0"), 
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", "1234567890"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.environ.get('GOOGLE_CLIENT_EMAIL')}"
    }
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(1) # Tab Anagrafica

    # 3. Lettura dati dal foglio
    raw_data = sheet.get_all_values()
    if len(raw_data) < 2: return "Foglio vuoto o intestazioni mancanti."
    
    headers_sheet = [h.strip() for h in raw_data[0]]
    try:
        idx_id = headers_sheet.index("ID Contatto")
    except:
        return "Errore: Colonna 'ID Contatto' non trovata nel foglio."

    log_finale = []
    
    # Definiamo la mappatura per costruire il payload
    # In base alla tua documentazione InsertAnagrafica
    mappatura_payload = {
        "ID": "ID Contatto",
        "codice": "Codice",
        "opzionale1": "Brand",
        "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR",
        "opzionale12": "Descrizione IT"
    }

    for r_idx, row_values in enumerate(raw_data[1:], start=2):
        item_id = str(row_values[idx_id]).strip()
        if not item_id: continue

        # Costruiamo l'oggetto anagrafica
        payload = {
            "IDDeposito": 1,
            "tipoArt": 0 # Articolo semplice
        }
        
        # Inseriamo i campi dalla riga del foglio
        for bman_key, sheet_title in mappatura_payload.items():
            try:
                col_idx = headers_sheet.index(sheet_title)
                payload[bman_key] = str(row_values[col_idx]).strip()
            except:
                continue

        esito_riga = ""
        
        # --- TENTATIVO 1: setAnagrafica (Update) ---
        soap_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
        
        try:
            r_set = requests.post(bman_url, data=soap_set, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}, timeout=30)
            res_set = ET.fromstring(r_set.content).find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            
            if res_set == "1":
                esito_riga = "✅ Aggiornato (setAnagrafica)"
            else:
                # --- TENTATIVO 2: Se fallisce, prova InsertAnagrafica (Create) ---
                soap_ins = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <InsertAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica>
    </InsertAnagrafica>
  </soap:Body>
</soap:Envelope>"""
                
                r_ins = requests.post(bman_url, data=soap_ins, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=30)
                res_ins = ET.fromstring(r_ins.content).find('.//{http://cloud.bman.it/}InsertAnagraficaResult').text
                
                if int(res_ins) > 0:
                    esito_riga = f"🆕 Creato (InsertAnagrafica ID: {res_ins})"
                else:
                    esito_riga = f"❌ Falliti entrambi (Err: {res_ins})"
        except Exception as e:
            esito_riga = f"⚠️ Errore tecnico: {str(e)}"

        log_finale.append(f"ID {item_id}: {esito_riga}")

    return "Sincronizzazione Bidirezionale terminata:\n" + "\n".join(log_finale)
