import os
import requests
import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # Variabili Bman e Google da Render
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    bman_url = f"{base_url}/getAnagrafiche"
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # Autenticazione Google semplificata
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "client_email": client_email,
        "private_key": private_key.replace('\\n', '\n'),
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    tutti_articoli = []
    pagina = 1
    
    # Ciclo di paginazione Bman (50 articoli per volta)
    while True:
        payload = {
            'chiave': bman_key,
            'filtri': json.dumps([]),
            'ordinamentoCampo': 'ID',
            'ordinamentoDirezione': 1,
            'numero di pagina': pagina,
            'listaDepositi': '',
            'dettaglioVarianti': 'False'
        }
        
        response = requests.post(bman_url, data=payload)
        if response.status_code != 200:
            raise Exception(f"Errore Bman: {response.status_code}")
            
        data = response.json()
        if not data or len(data) == 0:
            break
            
        tutti_articoli.extend(data)
        pagina += 1
        time.sleep(0.25) # Rispetto limite 5 req/sec

    # Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    header = ["ID", "Codice", "Descrizione", "Giacenza", "Prezzo Vendita"]
    rows = [header]
    
    for art in tutti_articoli:
        rows.append([
            art.get("ID"),
            art.get("codice"),
            art.get("descrizioneHtml", ""),
            art.get("giacenza"),
            art.get("przc")
        ])
    
    sheet.clear()
    sheet.update('A1', rows)
    
    return f"Sincronizzazione completata: {len(tutti_articoli)} articoli aggiornati!"
