import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def normalize_value(value):
    """Replica la logica normalizeValue del tuo script JS funzionante"""
    if value is None:
        return ""
    return str(value).strip().lower()

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # 2. Configurazione Credenziali Google (Dizionario Completo per evitare KeyError)
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
    
    articoli_filtrati = []
    pagina = 1
    valori_target = ["si", "approvato"] # Minuscoli per il confronto normalizzato
    
    # 3. Ciclo SOAP getAnagrafiche
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
</soap:Envelope>""".strip()

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
        }

        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
        
        if response.status_code != 200:
             raise Exception(f"Errore Bman a pagina {pagina}: Stato {response.status_code}")

        # Parsing XML e estrazione JSON
        tree = ET.fromstring(response.content)
        result_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
        
        if result_node is None or not result_node.text:
            break
            
        data = json.loads(result_node.text)
        
        if not data or len(data) == 0:
            break
            
        # 4. Filtro con normalizzazione
        for art in data:
            valore_opz11 = normalize_value(art.get("opzionale11"))
            if valore_opz11 in valori_target:
                articoli_filtrati.append(art)

        pagina += 1
        time.sleep(0.2) # Rispetto limite 5 req/sec

    # 5. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    header = ["ID", "Codice", "Descrizione", "Giacenza", "Prezzo", "Opzionale11"]
    rows = [header]
    
    for art in articoli_filtrati:
        rows.append([
            art.get("ID"), 
            art.get("codice"), 
            art.get("descrizioneHtml", ""), 
            art.get("giacenza"), 
            art.get("przc"),
            art.get("opzionale11")
        ])
    
    sheet.clear()
    if len(rows) > 1:
        # Usiamo l'ID del foglio per sicurezza
        sheet.update(rows) # Sintassi gspread moderna
        return f"Sincronizzazione completata! {len(articoli_filtrati)} articoli caricati."
    else:
        return "Nessun articolo trovato con opzionale11 = 'si' o 'Approvato'."
