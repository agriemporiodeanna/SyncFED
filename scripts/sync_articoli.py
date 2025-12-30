import os
import requests
import json
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # 2. Configurazione Google
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "1234567890"),
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "client_id": "1234567890",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 3. Definizione Filtro per Codice Specifico
    # Usiamo il codice da te fornito per trovare l'articolo esatto
    filtri = [
        {"chiave": "codice", "operatore": "=", "valore": "8032727740084"}
    ]

    # 4. Chiamata SOAP
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
    
    if response.status_code != 200:
        raise Exception(f"Errore Bman: Stato {response.status_code}")

    # Parsing XML per estrarre la stringa JSON
    tree = ET.fromstring(response.content)
    namespaces = {'ns': 'http://cloud.bman.it/'}
    result_node = tree.find('.//ns:getAnagraficheResult', namespaces)
    
    if result_node is None or not result_node.text:
        raise Exception("Nessun dato ricevuto da Bman per il codice fornito.")
        
    data = json.loads(result_node.text) #
    
    if not data or len(data) == 0:
        return f"Articolo {filtri[0]['valore']} non trovato in Bman."

    # 5. Estrazione nomi campi (Header)
    # Prendiamo le chiavi del primo oggetto restituito
    primo_articolo = data[0]
    intestazioni = list(primo_articolo.keys())
    valori_esempio = list(primo_articolo.values())
    
    # 6. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    # Scriviamo Intestazioni (Riga 1) e Dati Esempio (Riga 2) per verifica
    sheet.update('A1', [intestazioni, valori_esempio])
    
    return f"Successo! Trovato l'articolo {primo_articolo.get('codice')}. Intestazioni create."
