import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def normalize_value(value):
    """Replica esattamente la tua funzione JS: toglie spazi e rende minuscolo."""
    if value is None: return ""
    return str(value).strip().lower()

def clean_for_sheets(value):
    """Prepara i dati per Google Sheets."""
    if value is None: return ""
    if isinstance(value, (dict, list)): return json.dumps(value)
    return str(value)

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # 2. Setup Google
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 3. Mappatura Campi
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

    articoli_finali = []
    articoli_finali.append(list(mappatura.values()))

    pagina = 1
    totale_ricevuti = 0
    
    # 4. STEP 2 – SCRIPT = SI (OK)
    while True:
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[[]]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>{pagina}</numeroPagina>
      <listaDepositi><![CDATA[[]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

        headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}
        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
        
        tree = ET.fromstring(response.content)
        result_node = tree.find('.//{{http://cloud.bman.it/}}getAnagraficheResult')
        
        if result_node is None or not result_node.text: break
        
        articoli_pagina = json.loads(result_node.text)
        if not articoli_pagina: break
            
        totale_ricevuti += len(articoli_pagina)
        
        for a in articoli_pagina:
            # Replica esatta del filtro JS
            if normalize_value(a.get("opzionale11")) == 'si':
                riga = [clean_for_sheets(a.get(campo)) for campo in mappatura.keys()]
                articoli_finali.append(riga)

        pagina += 1
        time.sleep(0.2)
        if pagina > 100: break # Limite sicurezza

    # 5. Output
    print(f"📦 Articoli ricevuti: {totale_ricevuti}")
    print(f"✅ Articoli Script=SI: {len(articoli_finali)-1}")

    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"Sincronizzazione completata: {len(articoli_finali)-1} articoli su {totale_ricevuti} totali."
    else:
        return f"Analizzati {totale_ricevuti} prodotti, ma nessuno ha Script='si'."
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"Sincronizzazione completata! {len(articoli_finali)-1} articoli con Script='si' importati."
    else:
        return "Nessun prodotto trovato con Script='si' nel catalogo Bman."
