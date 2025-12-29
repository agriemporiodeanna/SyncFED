import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run():
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    if not all([client_email, private_key, sheet_id]):
        raise Exception("Mancano variabili d'ambiente Google su Render.")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Struttura completa per bypassare l'errore 'client_id'
    creds_dict = {
        "type": "service_account",
        "project_id": "sync-project",
        "private_key_id": "123456789",
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "client_id": "123456789", # ID fittizio necessario alla libreria
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"
    }
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        client.open_by_key(sheet_id)
        return "✅ Connessione Google Sheet OK!"
    except Exception as e:
        raise Exception(f"Errore di autenticazione Google: {str(e)}")
