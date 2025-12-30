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
        "strCategoria1": "Categoria1", "strCategoria2": "Categoria2"
    }

    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    
    # 2. Scarica dati correnti da Bman per confronto
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_get = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[1]]]></listaDepositi>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""
    
    headers_get = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}
    resp = requests.post(bman_url, data=soap_get, headers=headers_get, timeout=60)
    tree = ET.fromstring(resp.content)
    res_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
    bman_items = {str(i['ID']): i for i in json.loads(res_node.text)} if res_node is not None else {}

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
        
        # --- CALCOLO PREZZI PER VERIFICA COLORI ---
        try:
            iva_val = float(item_bman.get("iva", 0))
            p_lordo_bman = round(float(item_bman.get("przc", 0)) * (1 + iva_val/100), 2)
            p_min_target = round(p_lordo_bman * 0.8, 2)
            
            val_przb_sheet = float(str(row.get("Prezzo Minimo")).replace(',', '.'))
            val_przc_sheet = float(str(row.get("Prezzo")).replace(',', '.'))
            
            formats.append({"range": f"P{row_idx}", "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1} if abs(val_przb_sheet - p_min_target) < 0.01 else {"red": 1, "green": 0.8, "blue": 0.8}}})
            formats.append({"range": f"Q{row_idx}", "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1} if abs(val_przc_sheet - p_lordo_bman) < 0.01 else {"red": 1, "green": 0.8, "blue": 0.8}}})
        except: pass

        # --- CONTROLLO MODIFICHE ---
        for b_key, header in mappatura.items():
            if b_key in ["ID", "codice", "iva", "przc", "przb"]: continue
            
            val_s = str(row.get(header, "")).strip()
            val_b = str(item_bman.get(b_key, "")).strip()
            
            if val_s != val_b and val_s != "":
                nuovi_dati[b_key] = val_s
                campi_modificati.append(header)

        if campi_modificati:
            # Protezione prezzi: rimuoviamo przc e przb dall'invio
            nuovi_dati.pop('przc', None)
            nuovi_dati.pop('przb', None)
            nuovi_dati["IDDeposito"] = 1 # Obbligatorio per InsertAnagrafica

            # Usiamo InsertAnagrafica con l'ID esistente per fare l'Update
            soap_insert = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <InsertAnagrafica xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <anagrafica><![CDATA[{json.dumps(nuovi_dati)}]]></anagrafica>
    </InsertAnagrafica>
  </soap:Body>
</soap:Envelope>"""
            
            headers_insert = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/InsertAnagrafica'}
            res = requests.post(bman_url, data=soap_insert, headers=headers_insert, timeout=30)
            
            if res.status_code == 200 and "InsertAnagraficaResult" in res.text:
                log_azioni.append(f"ID {art_id}: AGGIORNATO ({', '.join(campi_modificati)})")
                prodotti_aggiornati += 1
            else:
                log_azioni.append(f"ID {art_id}: ERRORE INVIO")

    if formats:
        sheet.batch_format(formats)
    
    return f"Sincronizzazione completata.\nAggiornati: {prodotti_aggiornati}\n\nLOG:\n" + "\n".join(log_azioni if log_azioni else ["Nessuna modifica rilevata."])
