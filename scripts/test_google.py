import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def run():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")

    if not client_email or not private_key:
        raise Exception("Credenziali Google mancanti")

    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GOOGLE_PROJECT_ID", "syncfed"),
        "private_key_id": os.environ.get("GOOGLE_PRIVATE_KEY_ID", "dummy"),
        "private_key": private_key.replace("\\n", "\n"),
        "client_email": client_email,
        "client_id": os.environ.get("GOOGLE_CLIENT_ID", "dummy"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}",
    }

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise Exception("GOOGLE_SHEET_ID mancante")

    sh = client.open_by_key(sheet_id)
    ws = sh.get_worksheet(0)
    rows = ws.get_all_values()

    return {
        "status": "ok",
        "message": f"Connessione Google Sheet OK ({len(rows) - 1} righe dati)"
    }
