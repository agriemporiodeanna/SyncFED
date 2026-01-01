import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Configurazione Credenziali e Fogli
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
    
    # Punta al tab "Anagrafica" (Tab 2)
    try:
        sheet = workbook.get_worksheet(1)
    except:
        return "Errore: Il secondo Tab (Anagrafica) non esiste."

    # Mappatura Campi (BmanKey: Intestazione Sheet)
    mappatura = {
        "ID": "ID Contatto", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale6": "Titolo FR", "opzionale12": "Descrizione IT"
    }

    # 2. Lettura dati dal foglio
    raw_sheet = sheet.get_all_values()
    if len(raw_sheet) < 2: return "Foglio vuoto."
    
    headers = [h.strip() for h in raw_sheet[0]]
    idx_id = headers.index("ID Contatto") if "ID Contatto" in headers else 0

    # 3. Recupero dati correnti da bMan per avere i payload completi
    # Usiamo lo stesso filtro del pulsante 3 per coerenza
    filtri_bman = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri_bman)}]]></filtri>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""
    
    res_get = requests.post(bman_url, data=soap_get, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}, timeout=60)
    tree_get = ET.fromstring(res_get.content)
    json_bman = tree_get.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    db_bman = {str(item['ID']): item for item in json.loads(json_bman)} if json_bman else {}

    # 4. Confronto e Sincronizzazione
    log_finale = []
    formats = []
    aggiornati = 0

    for r_idx, row in enumerate(raw_sheet[1:], start=2):
        item_id = str(row[idx_id]).strip()
        if item_id not in db_bman: continue

        # Prendiamo la scheda originale COMPLETA da bMan
        payload_completo = db_bman[item_id]
        modificato = False

        for bman_key, sheet_header in mappatura.items():
            if sheet_header not in headers: continue
            
            c_idx = headers.index(sheet_header)
            val_sheet = str(row[c_idx]).strip()
            val_bman = str(payload_completo.get(bman_key, "")).strip()

            cell_ref = gspread.utils.rowcol_to_a1(r_idx, c_idx + 1)

            # Se il valore nel foglio è presente ed è diverso da bMan, aggiorniamo
            if val_sheet and val_sheet != val_bman:
                payload_completo[bman_key] = val_sheet
                modificato = True
                formats.append({"range": cell_ref, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}}) # Torna bianco
            elif not val_sheet:
                formats.append({"range": cell_ref, "format": {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}}}) # Resta rosso

        if modificato:
            # Invio della scheda completa aggiornata per evitare l'errore -23
            soap_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(payload_completo)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            res_set = requests.post(bman_url, data=soap_set, headers={'Content-Type': 'text/xml', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}, timeout=30)
            status = ET.fromstring(res_set.content).find('.//{http://cloud.bman.it/}setAnagraficaResult').text
            
            if status == "1":
                aggiornati += 1
                log_finale.append(f"ID {item_id}: Aggiornato con successo")
            else:
                log_finale.append(f"ID {item_id}: Errore bMan ({status})")

    # Applica formattazione colori
    if formats:
        sheet.batch_format(formats)

    if aggiornati == 0 and not log_finale:
        return "Nessuna modifica rilevata tra il foglio e bMan."

    return f"Sincronizzazione terminata. Aggiornati {aggiornati} articoli.\n" + "\n".join(log_finale)
