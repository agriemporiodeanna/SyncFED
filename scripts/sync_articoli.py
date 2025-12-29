import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def normalize_value(value):
    """Normalizzazione come da vecchio script JS"""
    if value is None: return ""
    return str(value).strip().lower()

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    bman_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # Configurazione Google completa per risolvere errore 'client_id'
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": "sync-project",
        "private_key_id": "123456789",
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "client_id": "123456789",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"
    }
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    
    articoli_filtrati = []
    pagina = 1
    valori_target = ["si", "approvato"]
    
    while True:
        # Template SOAP identico al sistema stabile
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

        resp = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
        if resp.status_code != 200: break

        tree = ET.fromstring(resp.content)
        result_node = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult')
        if result_node is None or not result_node.text: break
            
        data = json.loads(result_node.text)
        if not data: break
            
        for art in data:
            if normalize_value(art.get("opzionale11")) in valori_target:
                articoli_filtrati.append(art)

        pagina += 1
        time.sleep(0.25)

    header = ["ID", "Codice", "Descrizione", "Giacenza", "Prezzo", "Opzionale11"]
    rows = [header]
    for a in articoli_filtrati:
        rows.append([a.get("ID"), a.get("codice"), a.get("descrizioneHtml", ""), a.get("giacenza"), a.get("przc"), a.get("opzionale11")])
    
    sheet.clear()
    sheet.update(rows)
    return f"Sincronizzati {len(articoli_filtrati)} articoli (Script SI/Approvato)."
