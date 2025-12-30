import os, requests, json, gspread, re
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def clean_text(text):
    if not text: return ""
    # Pulisce spazi e normalizza maiuscole
    text = " ".join(str(text).split()).lower()
    return re.sub(r'(^|[.!?]\s+)([a-z])', lambda m: m.group(1) + m.group(2).upper(), text)

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    
    # Mappatura campi (CATEGORIE SEMPRE ESCLUSE DALL'INVIO)
    mappatura = {
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN", "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE"
    }

    # 1. Scarica i dati grezzi da bMan per il confronto reale 
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_get = f"""<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><getAnagrafiche xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><filtri><![CDATA[{json.dumps(filtri)}]]></filtri><numeroPagina>1</numeroPagina><listaDepositi><![CDATA[[1]]]></listaDepositi></getAnagrafiche></soap:Body></soap:Envelope>"""
    resp = requests.post(bman_url, data=soap_get, headers={'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}, timeout=60)
    bman_items = {str(i['ID']): i for i in json.loads(ET.fromstring(resp.content).find('.//{http://cloud.bman.it/}getAnagraficheResult').text)}

    sheet_rows = sheet.get_all_records()
    prodotti_aggiornati = 0

    for row in sheet_rows:
        art_id = str(row.get("ID")).strip()
        if art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        payload = item_bman.copy()
        modificato = False
        
        for b_key, header in mappatura.items():
            val_foglio_originale = str(row.get(header, "")).strip()
            val_bman_attuale = str(item_bman.get(b_key, "")).strip()
            
            # Calcoliamo come DEVE essere il valore finale 
            if header == "Brand":
                val_target = val_foglio_originale.upper()
            elif "Titolo" in header or "Descrizione" in header:
                val_target = clean_text(val_foglio_originale)
            else:
                val_target = val_foglio_originale

            # FORZA SCRITTURA: Se il valore su bMan non è identico al target (incluse maiuscole)
            if val_target != val_bman_attuale and val_target != "":
                payload[b_key] = val_target
                modificato = True

        if modificato:
            # Rimuove prezzi prima dell'invio per sicurezza
            payload.pop('przc', None)
            payload.pop('przb', None)
            payload["IDDeposito"] = 1
            
            soap_upd = f"""<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><InsertAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></InsertAnagrafica></soap:Body></soap:Envelope>"""
            requests.post(bman_url, data=soap_upd, headers={'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=30)
            prodotti_aggiornati += 1

    return f"Sincronizzazione completata. Aggiornati su bMan: {prodotti_aggiornati}"
