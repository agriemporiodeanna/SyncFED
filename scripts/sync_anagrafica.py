import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
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
    
    # 1. Recupero dati Bman
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_get = f"""<?xml version="1.0" encoding="utf-8"?>
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
        
        # --- VERIFICA PREZZI (SOLA LETTURA) ---
        try:
            iva_v = float(item_bman.get("iva", 0))
            p_lordo = round(float(item_bman.get("przc", 0)) * (1 + iva_v/100), 2)
            p_min_t = round(p_lordo * 0.8, 2)
            val_p_min = float(str(row.get("Prezzo Minimo")).replace(',', '.'))
            val_p_max = float(str(row.get("Prezzo")).replace(',', '.'))
            
            formats.append({"range": f"P{row_idx}", "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1} if abs(val_p_min - p_min_t) < 0.01 else {"red": 1, "green": 0.8, "blue": 0.8}}})
            formats.append({"range": f"Q{row_idx}", "format": {"backgroundColor": {"red": 1, "green": 1, "blue": 1} if abs(val_p_max - p_lordo) < 0.01 else {"red": 1, "green": 0.8, "blue": 0.8}}})
        except: pass

        # --- CONFRONTO CON FORZATURA MAIUSCOLO PER BRAND E CATEGORIE ---
        for b_key, header in mappatura.items():
            if b_key in ["ID", "codice", "iva", "przc", "przb"]: continue
            
            val_s = str(row.get(header, "")).strip()
            
            # Recupero valore Bman con gestione fallback nomi categoria
            val_b_raw = item_bman.get(b_key)
            if val_b_raw is None and "Categoria" in header:
                val_b_raw = item_bman.get(b_key.replace("strCategoria", "categoria") + "str")
            val_b = str(val_b_raw if val_b_raw is not None else "").strip()
            
            # Se è una categoria o il Brand, forziamo il maiuscolo per il confronto e l'invio
            if "Categoria" in header or header == "Brand":
                val_s = val_s.upper()
                val_b = val_b.upper()

            if val_s != val_b and val_s != "":
                nuovi_dati[b_key] = val_s
                campi_modificati.append(header)

        if campi_modificati:
            nuovi_dati.pop('przc', None)
            nuovi_dati.pop('przb', None)
            nuovi_dati["IDDeposito"] = 1 # Parametro obbligatorio

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
