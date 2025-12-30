import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def safe_value(value):
    if value is None: return ""
    if isinstance(value, (dict, list)): return json.dumps(value)
    return str(value)

def run():
    # 1. Recupero ENV
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # 2. Configurazione Google
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

    # Definiamo i filtri direttamente per Bman
    # Proviamo con 'si'. Se vuoi anche 'Approvato', Bman solitamente richiede filtri separati o operatore IN
    filtri_bman = [
        {"chiave": "opzionale11", "operatore": "=", "valore": "si"}
    ]

    righe_per_sheet = []
    intestazioni = list(mappatura.values()) + ["Foto1", "Foto2", "Foto3", "Foto4", "Foto5"]
    righe_per_sheet.append(intestazioni)

    pagina = 1
    
    while True:
        # Chiamata SOAP con filtro integrato
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <getAnagrafiche xmlns="http://cloud.bman.it/">
      <chiave>{bman_key}</chiave>
      <filtri><![CDATA[{json.dumps(filtri_bman)}]]></filtri>
      <ordinamentoCampo>ID</ordinamentoCampo>
      <ordinamentoDirezione>1</ordinamentoDirezione>
      <numeroPagina>{pagina}</numeroPagina>
      <listaDepositi><![CDATA[[]]]></listaDepositi>
      <dettaglioVarianti>false</dettaglioVarianti>
    </getAnagrafiche>
  </soap:Body>
</soap:Envelope>"""

        headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}
        
        try:
            response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
            tree = ET.fromstring(response.content)
            result_node = tree.find('.//{{http://cloud.bman.it/}}getAnagraficheResult')
            
            if result_node is None or not result_node.text: break
            data = json.loads(result_node.text) #
            
            if not data or len(data) == 0: break
                
            for art in data:
                riga = [safe_value(art.get(c)) for c in mappatura.keys()]
                # Estrazione Foto
                fotos = art.get("arrFoto", [])
                for i in range(5):
                    url_f = fotos[i].get("url", fotos[i].get("percorso", "")) if i < len(fotos) else ""
                    riga.append(url_f)
                righe_per_sheet.append(riga)

            pagina += 1
            time.sleep(0.2) #
        except Exception:
            break

    # 4. Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    if len(righe_per_sheet) > 1:
        sheet.update('A1', righe_per_sheet)
        return f"Sincronizzazione completata! Esportati {len(righe_per_sheet)-1} prodotti con Script='si'."
    else:
        return "Nessun prodotto trovato con opzionale11 = 'si'. Verifica il valore in Bman."
