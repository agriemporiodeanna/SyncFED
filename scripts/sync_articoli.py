import os
import requests
import json
import gspread
import time
import unicodedata
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def normalize_value(value):
    """Normalizzazione robusta (replica JS): toglie spazi, accenti e rende minuscolo."""
    if value is None: return ""
    s = str(value).strip().lower()
    s = unicodedata.normalize('NFD', s)
    return "".join(c for c in s if unicodedata.category(c) != 'Mn')

def clean_for_sheets(value):
    if value is None: return ""
    if isinstance(value, (dict, list)): return json.dumps(value)
    return str(value)

def run():
    # 1. Configurazione Ambiente
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = "https://emporiodeanna.bman.it/bmanapi.asmx"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # 2. Configurazione Google Sheets con tutti i campi obbligatori
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "1234567890abcdef"), # Campo ora incluso
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345678901234567890", # Campo ora incluso
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 3. Mappatura Campi
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

    # 4. Parametri conformi alla guida Bman (Deposito 1 obbligatorio)
    filtri_bman = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    deposito_bman = [1] 
    
    articoli_finali = [list(mappatura.values())]
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri_bman)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[{json.dumps(deposito_bman)}]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>""".strip()

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
    }

    response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
    tree = ET.fromstring(response.content)
    result_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
    
    if result_node is not None and result_node.text:
        articoli = json.loads(result_node.text)
        for a in articoli:
            if normalize_value(a.get("opzionale11")) == 'si':
                riga = [clean_for_sheets(a.get(campo)) for campo in mappatura.keys()]
                articoli_finali.append(riga)

    # 5. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"✅ Successo: Importati {len(articoli_finali)-1} articoli."
    else:
        return "⚠ Nessun articolo trovato con Script='si' nel Deposito 1."
    
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"✅ STEP 2 OK: {len(articoli_finali)-1} articoli Script=SI importati."
    else:
        return "❌ Nessun articolo trovato con i parametri del vecchio script."
