import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    # Se non hai queste variabili su Render, le inizializziamo a stringhe vuote 
    # per evitare l'errore 'KeyError' o di validazione della libreria
    project_id = os.environ.get("GOOGLE_PROJECT_ID", "sync-project")
    private_key_id = os.environ.get("GOOGLE_PRIVATE_KEY_ID", "123456789")

    if not all([sheet_id, client_email, private_key]):
        raise Exception("Mancano GOOGLE_SHEET_ID, GOOGLE_CLIENT_EMAIL o GOOGLE_PRIVATE_KEY su Render.")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # Struttura completa richiesta dalla libreria
        creds_dict = {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": private_key_id,
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
        
        sheet = client.open_by_key(sheet_id)
        return f"✅ Connessione OK! Accesso al foglio '{sheet.title}' confermato."
    
    except Exception as e:
        raise Exception(f"Errore Google: {str(e)}")
