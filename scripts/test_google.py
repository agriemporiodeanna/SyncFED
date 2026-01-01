from scripts.common_google import get_worksheet

SPREADSHEET_ID = "INSERISCI_ID_GOOGLE_SHEET"
SHEET_NAME = "ARTICOLI"
SERVICE_ACCOUNT_FILE = "service_account.json"

def run():
    ws = get_worksheet(SPREADSHEET_ID, SHEET_NAME, SERVICE_ACCOUNT_FILE)
    rows = ws.get_all_values()
    return {
        "status": "ok",
        "message": f"Google Sheet OK ({len(rows)-1} righe)"
    }
