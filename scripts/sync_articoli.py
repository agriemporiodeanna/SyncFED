import os, requests, json, gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Configurazione credenziali e accesso al foglio
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
    sheet = client.open_by_key(sheet_id).get_worksheet(0)

    # 2. Recupero dati da bMan 
    filtri = [{"chiave": "opzionale11", "operatore": "=", "valore": "si"}]
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
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
    
    headers = {'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'}
    resp = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    
    if not result_text: return "Nessun dato ricevuto da bMan."
    articoli = json.loads(result_text)
    
    # 3. Preparazione dati (Nessuna normalizzazione di testo applicata)
    data_to_write = []
    for art in articoli:
        iva = float(art.get("iva", 0))
        prezzo_lordo = round(float(art.get("przc", 0)) * (1 + iva/100), 2)
        prezzo_minimo = round(prezzo_lordo * 0.8, 2)

        row = [
            art.get("ID", ""),
            art.get("codice", ""),
            art.get("opzionale1", ""),                  # Brand
            art.get("opzionale2", ""),                  # Titolo IT
            art.get("opzionale5", ""),                  # Vinted
            art.get("opzionale6", ""),                  # Titolo FR
            art.get("opzionale7", ""),                  # Titolo EN
            art.get("opzionale8", ""),                  # Titolo ES
            art.get("opzionale9", ""),                  # Titolo DE
            art.get("opzionale11", ""),                 # Script
            art.get("opzionale12", ""),                 # Descrizione IT
            art.get("opzionale13", ""),                 # Descrizione FR
            art.get("opzionale14", ""),                 # Descrizione EN
            art.get("opzionale15", ""),                 # Descrizione ES
            art.get("opzionale16", ""),                 # Descrizione DE
            prezzo_minimo,
            prezzo_lordo,
            art.get("iva", ""),
            art.get("categoria1str", ""),               # Categoria 1
            art.get("categoria2str", "")                # Categoria 2
        ]
        data_to_write.append(row)

    # 4. Scrittura sul foglio
    header = ["ID", "Codice", "Brand", "Titolo IT", "Vinted", "Titolo FR", "Titolo EN", "Titolo ES", "Titolo DE", "Script", "Descrizione IT", "Descrizione FR", "Descrizione EN", "Descrizione ES", "Descrizione DE", "Prezzo Minimo", "Prezzo", "Iva", "Categoria1", "Categoria2"]
    sheet.clear()
    sheet.update('A1', [header] + data_to_write)
    
    return f"Scaricate {len(articoli)} anagrafiche. Dati ripristinati senza normalizzazione."
