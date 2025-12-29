import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run():
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = {
        "type": "service_account",
        "project_id": "sync-project",
        "private_key_id": "123456789",
        "private_key": private_key.replace('\\n', '\n'),
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gspread.authorize(creds).open_by_key(sheet_id)
    return "✅ Connessione Google Sheet OK!"
