import os, requests, json, gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
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
    articoli = json.loads(ET.fromstring(resp.content).find('.//{http://cloud.bman.it/}getAnagraficheResult').text)
    
    data_to_write = []
    for art in articoli:
        iva = float(art.get("iva", 0))
        p_lordo = round(float(art.get("przc", 0)) * (1 + iva/100), 2)
        
        row = [
            art.get("ID", ""), art.get("codice", ""), art.get("opzionale1", ""),
            art.get("opzionale2", ""), art.get("opzionale5", ""), art.get("opzionale6", ""),
            art.get("opzionale7", ""), art.get("opzionale8", ""), art.get("opzionale9", ""),
            art.get("opzionale11", ""), art.get("opzionale12", ""), art.get("opzionale13", ""),
            art.get("opzionale14", ""), art.get("opzionale15", ""), art.get("opzionale16", ""),
            round(p_lordo * 0.8, 2), p_lordo, art.get("iva", ""),
            art.get("categoria1str", ""), art.get("categoria2str", "")
        ]
        data_to_write.append(row)

    header = ["ID", "Codice", "Brand", "Titolo IT", "Vinted", "Titolo FR", "Titolo EN", "Titolo ES", "Titolo DE", "Script", "Descrizione IT", "Descrizione FR", "Descrizione EN", "Descrizione ES", "Descrizione DE", "Prezzo Minimo", "Prezzo", "Iva", "Categoria1", "Categoria2"]
    sheet.clear()
    sheet.update('A1', [header] + data_to_write)
    return f"Scaricate {len(articoli)} anagrafiche."
