import os
import requests
import json
import gspread
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
    
    mappatura = {
        "ID": "ID", "codice": "Codice",
        "opzionale1": "Brand", "opzionale2": "Titolo IT",
        "opzionale5": "Vinted", "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN", "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE", "opzionale11": "Script",
        "opzionale12": "Descrizione IT", "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN", "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE", 
        "przb": "Prezzo Minimo", "przc": "Prezzo", "iva": "Iva",
        "descrizioneHtml": "Descrizione Completa",
        "categoria1str": "Categoria1", "categoria2str": "Categoria2"
    }

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
    
    resp = requests.post(bman_url, data=soap_body, headers={'Content-Type': 'text/xml'}, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    articoli_bman = json.loads(result_text) if result_text else []

    righe_da_scrivere = [list(mappatura.values())]
    
    for art in articoli_bman:
        riga = []
        try:
            aliquota = float(art.get("iva", 0))
            prezzo_netto = float(art.get("przc", 0))
            prezzo_lordo = round(prezzo_netto * (1 + aliquota / 100), 2)
            # Calcolo Prezzo Minimo: Prezzo Lordo - 20%
            prezzo_minimo_lordo = round(prezzo_lordo * 0.80, 2)
        except:
            prezzo_lordo = 0.0
            prezzo_minimo_lordo = 0.0

        for b_key in mappatura.keys():
            if b_key == "przc":
                valore = prezzo_lordo
            elif b_key == "przb":
                valore = prezzo_minimo_lordo
            else:
                valore = art.get(b_key, "")
            riga.append(valore)
        righe_da_scrivere.append(riga)

    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    sheet.update('A1', righe_da_scrivere)
    
    return f"Scaricate {len(articoli_bman)} anagrafiche. Prezzo Minimo calcolato come Prezzo Lordo -20%."
