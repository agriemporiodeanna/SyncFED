import os
import requests
import json
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def clean_for_sheets(value):
    """Converte valori complessi in stringhe per Google Sheets"""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value) # Converte liste/dizionari in testo JSON
    return value

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "1234567890"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    filtri = [{"chiave": "codice", "operatore": "=", "valore": "8032727740084"}]
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>1</numeroPagina>
      <listaDepositi><![CDATA[[]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
    }

    response = requests.post(bman_url, data=soap_body, headers=headers, timeout=30)
    tree = ET.fromstring(response.content)
    result_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
    
    if result_node is None or not result_node.text:
        raise Exception("Nessun dato ricevuto da Bman")
        
    data = json.loads(result_node.text) #
    
    if not data:
        return "Articolo non trovato."

    primo_articolo = data[0]
    
    # 1. Crea le intestazioni (sempre stringhe)
    intestazioni = list(primo_articolo.keys())
    
    # 2. Pulisce i valori per Google Sheets
    valori_puliti = [clean_for_sheets(v) for v in primo_articolo.values()]
    
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    # Invia i dati come matrice (lista di liste)
    sheet.update('A1', [intestazioni, valori_puliti])
    
    return f"Successo! Articolo {primo_articolo.get('codice')} caricato correttamente."
