import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # 2. Configurazione Credenziali Google
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
    
    articoli_filtrati = []
    pagina = 1
    
    # Valori ammessi per il filtro
    valori_ammessi = ["si", "Approvato"]
    
    # 3. Ciclo SOAP
    while True:
        # Nota: Inviamo i filtri vuoti per scaricare i dati e filtrarli in Python
        # Questo garantisce che il filtro funzioni correttamente indipendentemente dalla sintassi SQL di Bman
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

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
        }

        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
        
        if response.status_code != 200:
             raise Exception(f"Errore Bman a pagina {pagina}: Stato {response.status_code}")

        tree = ET.fromstring(response.content)
        result_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
        
        if result_node is None or not result_node.text:
            break
            
        data = json.loads(result_node.text)
        
        if not data or len(data) == 0:
            break
            
        # --- LOGICA DI FILTRO LATO PYTHON ---
        for art in data:
            valore_campo = art.get("opzionale11", "")
            if valore_campo in valori_ammessi:
                articoli_filtrati.append(art)
        # ------------------------------------

        pagina += 1
        time.sleep(0.2) 

    # 4. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    header = ["ID", "Codice", "Descrizione", "Giacenza", "Prezzo Vendita", "Opzionale11"]
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
        sheet.update('A1', rows)
        return f"Sincronizzazione completata! {len(articoli_filtrati)} prodotti filtrati (si/Approvato) importati."
    else:
        return "Sincronizzazione completata, ma nessun prodotto corrisponde ai criteri (opzionale11 = si/Approvato)."
