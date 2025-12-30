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
        "ID": "ID", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN", "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE", 
        "przb": "Prezzo Minimo", "przc": "Prezzo", "iva": "Iva",
        "descrizioneHtml": "Descrizione Completa",
        "categoria1str": "Categoria1", "categoria2str": "Categoria2"
    }

    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(0)

    # 2. Scarica dati freschi da Bman
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

    # 3. Leggi Google Sheet
    sheet_rows = sheet.get_all_records()
    formats = []
    prodotti_aggiornati = 0

    for row_idx, row in enumerate(sheet_rows, start=2):
        art_id = str(row.get("ID")).strip()
        if art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        dati_per_aggiornamento = item_bman.copy()
        modificato = False
        
        # Recupero aliquota IVA corretta
        try:
            aliquota = float(item_bman.get("iva", 0))
        except:
            aliquota = 0

        for col_idx, (bman_key_attr, header_name) in enumerate(mappatura.items(), start=1):
            val_sheet_raw = str(row.get(header_name, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)
            
            # --- LOGICA PREZZI CON IVA AGGIUNTA ---
            if bman_key_attr in ["przc", "przb"]:
                try:
                    val_sheet_lordo = float(val_sheet_raw.replace(',', '.'))
                except:
                    val_sheet_lordo = 0.0
                
                # Calcolo del prezzo lordo REALE da Bman
                val_bman_netto = float(item_bman.get(bman_key_attr, 0))
                val_bman_lordo_reale = round(val_bman_netto * (1 + aliquota / 100), 2)

                # Se il prezzo nel foglio è 0 o diverso dal lordo calcolato -> ROSSO
                if val_sheet_lordo == 0 or abs(val_sheet_lordo - val_bman_lordo_reale) > 0.01:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
                else:
                    # Se è corretto e sincronizzato -> BIANCO
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
                continue # Importante: non aggiungiamo i prezzi alla coda di modifica

            # --- PROTEZIONE ID / CODICE / IVA ---
            if bman_key_attr in ["ID", "codice", "iva"]:
                val_bman = str(item_bman.get(bman_key_attr, "")).strip()
                color = {"red": 0.9, "green": 0.9, "blue": 0.9} if val_sheet_raw == val_bman else {"red": 1.0, "green": 0.6, "blue": 0.0}
                formats.append({"range": cell_range, "format": {"backgroundColor": color}})
                continue

            # --- ALTRI CAMPI TESTUALI (Titoli, Brand, Descrizioni) ---
            val_bman = str(item_bman.get(bman_key_attr, "")).strip()
            if not val_sheet_raw:
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
            elif val_sheet_raw != val_bman:
                dati_per_aggiornamento[bman_key_attr] = val_sheet_raw
                modificato = True
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
            else:
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})

        if modificato:
            # Rimuoviamo i prezzi dall'invio per evitare sovrascritture accidentali
            dati_per_aggiornamento.pop('przc', None)
            dati_per_aggiornamento.pop('przb', None)
            
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(dati_per_aggiornamento)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/setAnagrafica'}
            requests.post(bman_url, data=soap_body_set, headers=headers, timeout=30)
            prodotti_aggiornati += 1

    if formats:
        sheet.batch_format(formats)
    
    return f"Verifica completata. Aggiornati {prodotti_aggiornati} articoli. Prezzi lordi (IVA inclusa) verificati."
