import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # Recupero variabili da Render
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_dict = {
            "type": "service_account",
            "client_email": os.environ.get("GOOGLE_CLIENT_EMAIL"),
            "private_key": os.environ.get("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # Prova ad aprire il foglio
        sheet = client.open_by_key(sheet_id)
        return f"Connessione OK! Foglio trovato: '{sheet.title}'"
    
    except Exception as e:
        raise Exception(f"Errore di connessione Google: {str(e)}")
