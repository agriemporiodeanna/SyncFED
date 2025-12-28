import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run():
    # Recupero variabili presenti su Render
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    client_email = os.environ.get("GOOGLE_CLIENT_EMAIL")
    private_key = os.environ.get("GOOGLE_PRIVATE_KEY")
    
    if not all([sheet_id, client_email, private_key]):
        raise Exception("Verifica che GOOGLE_SHEET_ID, GOOGLE_CLIENT_EMAIL e GOOGLE_PRIVATE_KEY siano impostati su Render.")

    try:
        # Definiamo i permessi necessari
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # Creiamo un set minimo di credenziali per evitare l'errore 'private_key_id'
        creds_dict = {
            "type": "service_account",
            "client_email": client_email,
            "private_key": private_key.replace('\\n', '\n'),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        
        # Utilizziamo ServiceAccountCredentials bypassando la validazione dei campi non essenziali
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # Test di apertura foglio
        sheet = client.open_by_key(sheet_id)
        return f"✅ Connessione OK! Accesso al foglio '{sheet.title}' confermato."
    
    except Exception as e:
        # Se l'errore persiste, forniamo un messaggio più chiaro
        error_msg = str(e)
        if 'private_key_id' in error_msg:
            return "❌ Errore interno: La libreria richiede campi extra. Prova a ricreare le chiavi JSON su Google Cloud."
        raise Exception(f"Errore Google: {error_msg}")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # Prova ad aprire il foglio
        sheet = client.open_by_key(sheet_id)
        return f"Connessione OK! Foglio trovato: '{sheet.title}'"
    
    except Exception as e:
        raise Exception(f"Errore di connessione Google: {str(e)}")
