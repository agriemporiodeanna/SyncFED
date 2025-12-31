import os
import requests
import json
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # Configurazione Fogli e Credenziali
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(1) 

    # Lettura dati dal foglio
    raw_data = sheet.get_all_values()
    headers_sheet = [h.strip() for h in raw_data[0]]
    idx_id = headers_sheet.index("ID Contatto")
    
    log_finale = []

    for r_idx, row in enumerate(raw_data[1:], start=2):
        item_id = str(row[idx_id]).strip()
        if not item_id: continue

        # Prepariamo i dati (mappatura semplificata per l'esempio)
        payload = {
            "ID": item_id,
            "opzionale1": str(row[headers_sheet.index("Brand")]).strip(),
            "opzionale2": str(row[headers_sheet.index("Titolo IT")]).strip(),
            "opzionale6": str(row[headers_sheet.index("Titolo FR")]).strip(),
            "IDDeposito": 1,
            "tipoArt": 0  # Articolo semplice
        }

        successo = False
        metodo_usato = ""

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
            resp = requests.post(bman_url, data=soap_set, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}, timeout=20)
            res_val = ET.fromstring(resp.content).find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            if int(res_val) > 0:
                successo = True
                metodo_usato = "setAnagrafica (Aggiornamento)"
        except:
            res_val = "Errore Connessione"

        # --- TENTATIVO 2: Se il primo fallisce, prova InsertAnagrafica (Create) ---
        if not successo:
            soap_insert = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <InsertAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica>
    </InsertAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            try:
                resp = requests.post(bman_url, data=soap_insert, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=20)
                res_val_ins = ET.fromstring(resp.content).find('.//{http://cloud.bman.it/}InsertAnagraficaResult').text
                if int(res_val_ins) > 0:
                    successo = True
                    metodo_usato = "InsertAnagrafica (Creazione)"
                else:
                    metodo_usato = f"Falliti entrambi (Errore: {res_val_ins})"
            except:
                metodo_usato = "Errore critico durante Insert"

        log_finale.append(f"ID {item_id}: {metodo_usato}")

    return "Esito Sincronizzazione:\n" + "\n".join(log_finale)
