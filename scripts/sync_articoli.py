import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def safe_str(value):
    """Converte qualsiasi valore in stringa pulita per Google Sheets."""
    if value is None: return ""
    if isinstance(value, (dict, list)): return json.dumps(value)
    return str(value).strip()

def run():
    # 1. Recupero Credenziali Ambiente
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    # 2. Setup Google Sheets con workaround per i campi 'private_key_id' e 'client_id'
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "bman-sync"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"),
        "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
        "client_id": "12345",
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # 3. Mappatura Campi (Bman -> Google Sheet)
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
    # Costruzione intestazioni: ID, Codice, Foto1..5, e poi il resto
    intestazioni = ["ID", "Codice", "Foto1", "Foto2", "Foto3", "Foto4", "Foto5"] + list(mappatura.values())[2:]
    articoli_finali.append(intestazioni)

    pagina = 1
    
    while True:
        # Chiamata SOAP getAnagrafiche senza filtri per massimizzare la compatibilità
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
        
        # Parsing della risposta XML
        tree = ET.fromstring(response.content)
        result_node = tree.find('.//{{http://cloud.bman.it/}}getAnagraficheResult')
        
        if result_node is None or not result_node.text: break
        data = json.loads(result_node.text)
        if not data or len(data) == 0: break
            
        for art in data:
            # Filtro lato Python: verifica che Script sia "si" o "Approvato"
            val_script = str(art.get("opzionale11", "")).strip().lower()
            if val_script in ["si", "approvato"]:
                riga = []
                riga.append(safe_str(art.get("ID")))
                riga.append(safe_str(art.get("codice")))
                
                # Estrazione URL Foto (Foto1-Foto5) dal campo arrFoto
                fotos = art.get("arrFoto", [])
                for i in range(5):
                    url_f = fotos[i].get("url", "") if i < len(fotos) else ""
                    riga.append(url_f)
                
                # Resto dei campi mappati (saltando ID e Codice già inseriti)
                chiavi_mappate = list(mappatura.keys())
                for campo_bman in chiavi_mappate[2:]:
                    riga.append(safe_str(art.get(campo_bman)))
                
                articoli_finali.append(riga)

        pagina += 1
        time.sleep(0.2) # Rispetto del limite 5 req/sec
        if pagina > 50: break # Limite di sicurezza per il test

    # 4. Aggiornamento Foglio Google
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"Sincronizzazione completata: {len(articoli_finali)-1} prodotti esportati correttamente."
    else:
        return "Nessun prodotto trovato con Script='si' nel catalogo scaricato."
