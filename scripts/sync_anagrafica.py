import os, requests, json, gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Configurazione Iniziale
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
    
    # Mappatura: Gestiamo Titoli e Descrizioni in modo diretto
    # NOTA: Categorie escluse dall'invio come richiesto
    mappatura = {
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN", "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE"
    }

    # 2. Recupero dati da bMan per il confronto
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_get = f"""<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><getAnagrafiche xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><filtri><![CDATA[{json.dumps(filtri)}]]></filtri><numeroPagina>1</numeroPagina><listaDepositi><![CDATA[[1]]]></listaDepositi></getAnagrafiche></soap:Body></soap:Envelope>"""
    resp_get = requests.post(bman_url, data=soap_get, headers={'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}, timeout=60)
    bman_items = {str(i['ID']): i for i in json.loads(ET.fromstring(resp_get.content).find('.//{http://cloud.bman.it/}getAnagraficheResult').text)}

    sheet_rows = sheet.get_all_records()
    prodotti_aggiornati = 0
    log_dettagli = []

    for row in sheet_rows:
        art_id = str(row.get("ID")).strip()
        if art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        payload = item_bman.copy()
        modificato = False
        
        for b_key, header in mappatura.items():
            val_foglio = str(row.get(header, "")).strip()
            val_bman = str(item_bman.get(b_key, "")).strip()
            
            # Confronto testuale semplice (Case Sensitive)
            if val_foglio != val_bman and val_foglio != "":
                payload[b_key] = val_foglio
                modificato = True

        if modificato:
            # Rimozione campi protetti prima dell'invio
            payload.pop('przc', None)
            payload.pop('przb', None)
            payload["IDDeposito"] = 1
            
            # Invio tramite InsertAnagrafica (Upsert)
            soap_insert = f"""<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><InsertAnagrafica xmlns="http://cloud.bman.it/"><chiave>{bman_key}</chiave><anagrafica><![CDATA[{json.dumps(payload)}]]></anagrafica></InsertAnagrafica></soap:Body></soap:Envelope>"""
            res = requests.post(bman_url, data=soap_insert, headers={'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}, timeout=30)
            
            # Controllo diagnostico della risposta
            if res.status_code == 200:
                prodotti_aggiornati += 1
                log_dettagli.append(f"ID {art_id}: AGGIORNATO")
            else:
                log_dettagli.append(f"ID {art_id}: ERRORE HTTP {res.status_code}")

    res_msg = f"Sincronizzazione completata. Prodotti processati: {prodotti_aggiornati}\n\nLOG:\n" + "\n".join(log_dettagli if log_dettagli else ["Nessuna modifica rilevata."])
    return res_msg
