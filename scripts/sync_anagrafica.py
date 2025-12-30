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

    # 3. Leggi Google Sheet
    sheet_rows = sheet.get_all_records()
    formats = []
    prodotti_aggiornati = 0

    for row_idx, row in enumerate(sheet_rows, start=2):
        art_id = str(row.get("ID")).strip()
        if art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        modificato = False
        dati_agg_bman = item_bman.copy()
        
        # Recupero aliquota IVA (es. 22)
        try:
            aliquota = float(item_bman.get("iva", 0))
        except:
            aliquota = 0

        for col_idx, (bman_key_attr, header_name) in enumerate(mappatura.items(), start=1):
            val_sheet_raw = str(row.get(header_name, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)
            
            # --- GESTIONE PREZZI (Calcolo Lordo) ---
            if bman_key_attr in ["przc", "przb"]:
                try:
                    val_sheet = float(val_sheet_raw.replace(',', '.'))
                except:
                    val_sheet = 0.0
                
                # Calcoliamo il lordo da Bman (Netto * (1 + Aliquota/100))
                val_bman_netto = float(item_bman.get(bman_key_attr, 0))
                val_bman_lordo = round(val_bman_netto * (1 + aliquota / 100), 2)

                if val_sheet == 0:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
                elif abs(val_sheet - val_bman_lordo) > 0.01:
                    # Se l'utente ha cambiato il prezzo lordo nel foglio, scorporiamo l'IVA per Bman
                    nuovo_netto = val_sheet / (1 + aliquota / 100)
                    dati_agg_bman[bman_key_attr] = nuovo_netto
                    modificato = True
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
                else:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})

            # --- PROTEZIONE ID/CODICE ---
            elif bman_key_attr in ["ID", "codice"]:
                val_bman = str(item_bman.get(bman_key_attr, "")).strip()
                if val_sheet_raw != val_bman:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}}})
                else:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}}})

            # --- ALTRI CAMPI ---
            else:
                val_bman = str(item_bman.get(bman_key_attr, "")).strip()
                if not val_sheet_raw:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
                elif val_sheet_raw != val_bman:
                    dati_agg_bman[bman_key_attr] = val_sheet_raw
                    modificato = True
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
                else:
                    formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})

        if modificato:
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(dati_agg_bman)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            requests.post(bman_url, data=soap_body_set, headers={'Content-Type': 'text/xml'}, timeout=30)
            prodotti_aggiornati += 1

    if formats:
        sheet.batch_format(formats)
    
    return f"Sync completato. {prodotti_aggiornati} articoli aggiornati. I prezzi a zero sono evidenziati in rosso."
