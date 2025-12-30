import os
import requests
import json
import gspread
import time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

def normalize_value(value):
    if value is None: return ""
    return str(value).strip().lower()

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
    
    # 3. Mappatura Campi (Bman -> Intestazione Google Sheet)
    mappatura = {
        "ID": "ID",
        "codice": "Codice",
        "opzionale1": "Brand",
        "opzionale2": "Titolo IT",
        "opzionale5": "Vinted",
        "opzionale6": "Titolo FR",
        "opzionale7": "Titolo EN",
        "opzionale8": "Titolo ES",
        "opzionale9": "Titolo DE",
        "opzionale11": "Script",
        "opzionale12": "Descrizione IT",
        "opzionale13": "Descrizione FR",
        "opzionale14": "Descrizione EN",
        "opzionale15": "Descrizione ES",
        "opzionale16": "Descrizione DE",
        "przb": "Prezzo Minimo",
        "przc": "Prezzo",
        "iva": "Iva",
        "descrizioneHtml": "Descrizione Completa",
        "categoria1str": "Categoria1",
        "categoria2str": "Categoria2"
    }

    articoli_finali = []
    intestazioni = list(mappatura.values())
    articoli_finali.append(intestazioni) # Aggiunge la riga delle intestazioni

    pagina = 1
    valori_target = ["si", "approvato"] # Filtro opzionale11
    
    while True:
        # Chiamata SOAP
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

        headers = {{
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': 'http://cloud.bman.it/getAnagrafiche'
        }}

        response = requests.post(bman_url, data=soap_body, headers=headers, timeout=60)
        tree = ET.fromstring(response.content)
        result_node = tree.find('.//{{http://cloud.bman.it/}}getAnagraficheResult')
        
        if result_node is None or not result_node.text: break
        data = json.loads(result_node.text)
        if not data: break
            
        for art in data:
            # Filtro opzionale11 (Script)
            if normalize_value(art.get("opzionale11")) in valori_target:
                riga = []
                # Estrae solo i campi mappati nell'ordine corretto
                for campo_bman in mappatura.keys():
                    valore = art.get(campo_bman, "")
                    # Gestione speciale per arrFoto (se serve espanderlo in futuro)
                    riga.append(str(valore) if valore is not None else "")
                articoli_finali.append(riga)

        pagina += 1
        time.sleep(0.2) 

    # 4. Scrittura su Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    sheet.clear()
    
    if len(articoli_finali) > 1:
        sheet.update('A1', articoli_finali)
        return f"Sincronizzazione completata! {len(articoli_finali)-1} prodotti mappati importati."
    else:
        return "Nessun prodotto trovato con opzionale11 = 'si' o 'Approvato'."
    
    # Invia i dati come matrice (lista di liste)
    sheet.update('A1', [intestazioni, valori_puliti])
    
    return f"Successo! Articolo {primo_articolo.get('codice')} caricato correttamente."
