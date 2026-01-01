import requests

BMAN_BASE_URL = "https://DOMINIO.bman.it:3555/bmanapi.asmx"
BMAN_KEY = "INSERISCI_CHIAVE_BMAN"

def run():
    payload = {
        "chiave": BMAN_KEY,
        "numeroPagina": 1
    }
    r = requests.post(f"{BMAN_BASE_URL}/getAnagrafiche", json=payload, timeout=15)
    r.raise_for_status()
    return {"status": "ok", "message": "Connessione BMAN OK"}
