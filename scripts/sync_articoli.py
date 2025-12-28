import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Recupero ENV da Render
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # 2. Configurazione Credenziali Google (Dizionario Completo)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "1234567890"),
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "client_id": "1234567890",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"
    }
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    tutti_articoli = []
    pagina = 1
    
    # 3. Ciclo di estrazione dati SOAP Bman
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
      <listaDepositi><![CDATA[]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
        }

        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=30)
        
        # Se ricevi HTML invece di XML, interrompi con errore chiaro
        if response.status_code != 200:
             raise Exception(f"Errore Bman: Il server ha risposto con codice {response.status_code}. Verifica l'URL.")
        
        if "<html>" in response.text:
             raise Exception("Il server Bman ha restituito una pagina HTML invece di dati. Controlla che l'endpoint sia corretto.")

        # Parsing XML per estrarre il JSON
        try:
            tree = ET.fromstring(response.content)
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                'ns': 'http://cloud.bman.it/'
            }
            result_node = tree.find('.//ns:getAnagraficheResult', namespaces)
            
            if result_node is None or not result_node.text:
                break
                
            data = json.loads(result_node.text) # Converte la stringa CDATA in JSON
        except Exception as e:
            raise Exception(f"Errore nel processare la risposta della pagina {pagina}: {str(e)}")
        
        if not data or len(data) == 0:
            break
            
        tutti_articoli.extend(data)
        pagina += 1
        time.sleep(0.3) # Limite massimo 5 richieste al secondo

    # 4. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    
    header = ["ID", "Codice", "Descrizione", "Giacenza", "Prezzo Vendita (przc)"]
    rows = [header]
    
    for art in tutti_articoli:
        rows.append([
            art.get("ID"), 
            art.get("codice"), 
            art.get("descrizioneHtml", ""), 
            art.get("giacenza"), 
            art.get("przc")
        ])
    
    sheet.clear()
    sheet.update('A1', rows)
    
    return f"Sincronizzazione completata! {len(tutti_articoli)} articoli importati."
