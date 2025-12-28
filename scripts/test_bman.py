import os
import requests
import json

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    
    if not bman_key or not base_url:
        raise Exception("Variabili BMAN_API_KEY o BMAN_BASE_URL mancanti.")

    # Costruzione URL pulito
    clean_url = base_url.strip().rstrip('/')
    bman_url = f"{clean_url}/getAnagrafiche"
    
    # Parametri obbligatori
    params = {
        'chiave': bman_key,
        'filtri': json.dumps([]),
        'ordinamentoCampo': 'ID',
        'ordinamentoDirezione': '1',
        'numero di pagina': '1',
        'listaDepositi': '',
        'dettaglioVarianti': 'false'
    }

    try:
        # Proviamo a inviare i parametri sia nel body che come query string 
        # per forzare il riconoscimento da parte di ASP.NET
        response = requests.post(
            bman_url, 
            params=params, # Query String
            data=params,   # Form Data
            timeout=15
        )
        
        if response.status_code == 200:
            return "✅ Connessione Bman OK! Il server ha riconosciuto il formato."
        else:
            return f"❌ Errore Bman {response.status_code}: Formato non riconosciuto. Verifica l'URL."
            
    except Exception as e:
        raise Exception(f"❌ Errore tecnico: {str(e)}")
