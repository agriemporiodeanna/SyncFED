import os
import requests
import json

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    
    if not bman_key or not base_url:
        raise Exception("Variabili BMAN_API_KEY o BMAN_BASE_URL mancanti su Render.")

    # Costruzione URL pulito
    bman_url = base_url.strip().rstrip('/')
    if not bman_url.endswith('/getAnagrafiche'):
        bman_url = f"{bman_url}/getAnagrafiche"
    
    # Parametri richiesti esplicitamente dalla documentazione
    payload = {
        'chiave': bman_key,
        'filtri': json.dumps([]),
        'ordinamentoCampo': 'ID',
        'ordinamentoDirezione': 1, # 1 – ASC
        'numero di pagina': 1,     # Nome parametro con spazi come da doc
        'listaDepositi': '',
        'dettaglioVarianti': 'false'
    }

    try:
        # Forziamo gli header per simulare un form classico
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0'
        }
        
        # Invio della richiesta
        response = requests.post(bman_url, data=payload, headers=headers, timeout=15)
        
        if response.status_code == 200:
            return "✅ Connessione Bman OK! Il server ha accettato i parametri."
        else:
            # Stampiamo il contenuto della risposta per capire l'errore ASP.NET
            return f"❌ Errore Bman {response.status_code}: {response.text[:100]}"
            
    except Exception as e:
        raise Exception(f"❌ Errore tecnico: {str(e)}")
