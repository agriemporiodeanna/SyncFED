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
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    mappatura = {
        "ID": "ID", "codice": "Codice", "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR", "opzionale7": "Titolo EN",
        "opzionale8": "Titolo ES", "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE", "przb": "Prezzo Minimo", "przc": "Prezzo", 
        "iva": "Iva", "descrizioneHtml": "Descrizione Completa",
        "categoria1str": "Categoria1", "categoria2str": "Categoria2"
    }

    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    
    # 2. Scarica dati da Bman
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_body_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri)}]]></filtri>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""
    
    resp = requests.post(bman_url, data=soap_body_get, headers={'Content-Type': 'text/xml'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    bman_items = {str(i['ID']): i for i in json.loads(result_text)} if result_text else {}

    sheet_rows = sheet.get_all_records()
    formats = []
    log_azioni = []
    prodotti_aggiornati = 0

    for row_idx, row in enumerate(sheet_rows, start=2):
        art_id = str(row.get("ID")).strip()
        if art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        nuovi_dati = item_bman.copy()
        campi_modificati = []
        bloccato = False
        
        # Calcolo prezzi per verifica colori
        try:
            iva = float(item_bman.get("iva", 0))
            p_lordo_bman = round(float(item_bman.get("przc", 0)) * (1 + iva/100), 2)
            p_min_target = round(p_lordo_bman * 0.8, 2)
        except:
            p_lordo_bman = p_min_target = 0

        for col_idx, (b_key, header) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(header, "")).strip()
            val_bman = str(item_bman.get(b_key, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)

            # --- PREZZI: Solo Colore, NO Invio ---
            if b_key in ["przc", "przb"]:
                v_s_num = float(val_sheet.replace(',', '.')) if val_sheet else 0.0
                target = p_lordo_bman if b_key == "przc" else p_min_target
                if abs(v_s_num - target) > 0.01 or v_s_num == 0:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
                else:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}})
                continue

            # --- PROTEZIONI CHIAVE ---
            if b_key in ["ID", "codice"]:
                if val_sheet != val_bman:
                    bloccato = True
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}}})
                else:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}})
                continue

            # --- CAMPI MODIFICABILI ---
            if val_sheet != val_bman and val_sheet != "":
                nuovi_dati[b_key] = val_sheet
                campi_modificati.append(header)
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}})
            elif not val_sheet:
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
            else:
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}})

        # --- ESECUZIONE INVIO ---
        if bloccato:
            log_azioni.append(f"ID {art_id}: BLOCCATO (ID/Codice manomessi)")
        elif campi_modificati:
            # Rimuoviamo i prezzi prima dell'invio come richiesto
            nuovi_dati.pop('przc', None)
            nuovi_dati.pop('przb', None)
            
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(nuovi_dati)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            h = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}
            res = requests.post(bman_url, data=soap_body_set, headers=h, timeout=30)
            
            if res.status_code == 200:
                log_azioni.append(f"ID {art_id}: AGGIORNATO (Campi: {', '.join(campi_modificati)})")
                prodotti_aggiornati += 1
            else:
                log_azioni.append(f"ID {art_id}: ERRORE SERVER BMAN")

    if formats:
        sheet.batch_format(formats)
    
    res_msg = f"Sincronizzazione completata.\nProdotti aggiornati: {prodotti_aggiornati}\n\nDETTAGLIO AZIONI:\n" + "\n".join(log_azioni if log_azioni else ["Nessuna modifica rilevata nei campi anagrafici."])
    return res_msg
