import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_worksheet(spreadsheet_id, sheet_name, service_account_file):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        service_account_file,
        scope
    )

    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id).worksheet(sheet_name)
