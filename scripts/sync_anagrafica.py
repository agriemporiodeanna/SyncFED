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
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 2. Mappatura Integrale (Corrispondenza esatta con il tuo Google Sheet)
    mappatura = {
        "ID": "ID", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN", "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE", "przb": "Prezzo Minimo",
        "przc": "Prezzo", "iva": "Iva",
        "descrizioneHtml": "Descrizione Completa",
        "categoria1str": "Categoria1", "categoria2str": "Categoria2"
    }

    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(0) # Primo tab dove sono gli articoli

    # 3. Scarica dati da Bman (Solo approvati Script=si)
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_body_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""
    
    resp = requests.post(bman_url, data=soap_body_get, headers={'Content-Type': 'text/xml'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    bman_items = {str(i['ID']): i for i in json.loads(result_text)} if result_text else {}

    # 4. Leggi Google Sheet e analizza ogni cella
    sheet_rows = sheet.get_all_records()
    formats = []
    prodotti_aggiornati = 0

    for row_idx, row in enumerate(sheet_rows, start=2):
        art_id = str(row.get("ID"))
        if not art_id or art_id not in bman_items: continue
        
        item_bman = bman_items[art_id]
        modificato_per_questo_prodotto = False
        dati_aggiornati_bman = item_bman.copy()

        # Cicliamo su ogni campo della mappatura
        for col_idx, (bman_key_attr, header_name) in enumerate(mappatura.items(), start=1):
            val_sheet = str(row.get(header_name, "")).strip()
            val_bman = str(item_bman.get(bman_key_attr, "")).strip()
            cell_range = gspread.utils.rowcol_to_a1(row_idx, col_idx)

            # Logica dei Colori
            if not val_sheet:
                # Rosso se la cella è vuota (manca un dato)
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}})
            elif val_sheet != val_bman:
                # Se c'è un dato nuovo nel foglio diverso da Bman, aggiorniamo
                dati_aggiornati_bman[bman_key_attr] = val_sheet
                modificato_per_questo_prodotto = True
                # Bianco perché ora il dato è pronto per essere allineato
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})
            else:
                # Bianco se i dati coincidono già
                formats.append({"range": cell_range, "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}})

        # Se almeno un campo del prodotto è cambiato, inviamo a Bman
        if modificato_per_questo_prodotto:
            soap_body_set = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <setAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(dati_aggiornati_bman)}]]></anagrafica>
    </setAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            requests.post(bman_url, data=soap_body_set, headers={'Content-Type': 'text/xml'}, timeout=30)
            prodotti_aggiornati += 1

    # 5. Applica formattazione colori in un colpo solo (veloce)
    if formats:
        sheet.batch_format(formats)
    
    return f"Sincronizzazione completata. {prodotti_aggiornati} articoli aggiornati in Bman. Celle vuote evidenziate in rosso."
