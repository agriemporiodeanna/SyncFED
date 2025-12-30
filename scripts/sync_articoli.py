import os
import requests
import json
import gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # 1. Configurazione e Credenziali
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
    
    # Mappatura Campi (Stessa del Pulsante 4)
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

    # 2. Recupero Dati da Bman (Filtro Script = si)
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
    
    headers = {'Content-Type': 'text/xml; charset=utf-8'}
    resp = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
    tree = ET.fromstring(resp.content)
    result_text = tree.find('.//{http://cloud.bman.it/}getAnagraficheResult').text
    
    if not result_text:
        return "Nessun articolo trovato con Script='si'."
    
    articoli_bman = json.loads(result_text)

    # 3. Preparazione Righe per Google Sheet con Calcolo IVA
    righe_da_scrivere = [list(mappatura.values())] # Intestazioni
    
    for art in articoli_bman:
        riga = []
        # Recupero Aliquota IVA per l'articolo
        try:
            aliquota = float(art.get("iva", 0))
        except:
            aliquota = 0

        for bman_key in mappatura.keys():
            valore = art.get(bman_key, "")
            
            # --- LOGICA CALCOLO PREZZO LORDO ---
            if bman_key in ["przc", "przb"]:
                try:
                    prezzo_netto = float(valore) if valore else 0.0
                    valore = round(prezzo_netto * (1 + aliquota / 100), 2)
                except:
                    valore = 0.0
            
            riga.append(valore)
        righe_da_scrivere.append(riga)

    # 4. Scrittura su Google Sheet
    workbook = client.open_by_key(sheet_id)
    sheet = workbook.get_worksheet(0)
    sheet.clear()
    sheet.update('A1', righe_da_scrivere)
    
    return f"Sincronizzazione completata: {len(articoli_bman)} articoli scaricati con prezzi IVATI."
