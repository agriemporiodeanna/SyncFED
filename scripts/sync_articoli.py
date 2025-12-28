import os
import requests
import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    bman_url = base_url.strip().rstrip('/')
    if not bman_url.endswith('/getAnagrafiche'):
        bman_url = f"{bman_url}/getAnagrafiche"

    # Autenticazione Google Sheet con gestione errori private_key_id
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "sync-project"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "12345"),
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    tutti_articoli = []
    pagina = 1
    
    # Header per Bman
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    while True:
        payload = {
            'chiave': bman_key,
            'filtri': json.dumps([]),
            'ordinamentoCampo': 'ID',
            'ordinamentoDirezione': 1,
            'numero di pagina': pagina, #
            'listaDepositi': '',
            'dettaglioVarianti': 'false'
        }
        
        response = requests.post(bman_url, data=payload, headers=headers, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"Errore Bman a pagina {pagina}: {response.text[:100]}")
            
        data = response.json()
        if not data or len(data) == 0:
            break
            
        tutti_articoli.extend(data)
        pagina += 1
        time.sleep(0.3) # Rispetto limite 5 req/sec

    # Scrittura su Google Sheet
    sheet = client.open_by_key(sheet_id).get_worksheet(0)
    # Intestazione basata sulla risposta tipo di Bman
    header = ["ID", "Codice", "Giacenza", "Prezzo Vendita (przc)"]
    rows = [header]
    
    for art in tutti_articoli:
        rows.append([
            art.get("ID"), 
            art.get("codice"), 
            art.get("giacenza"), 
            art.get("przc") #
        ])
    
    sheet.clear()
    sheet.update('A1', rows)
    
    return f"Sincronizzazione completata: {len(tutti_articoli)} articoli importati."
