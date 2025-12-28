import os
import requests
import json

def run():
    # Recupero variabili da Render
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    
    if not bman_key or not base_url:
        raise Exception("Variabili BMAN_API_KEY o BMAN_BASE_URL mancanti su Render")

    # Pulizia URL: rimuove eventuali spazi o slash finali e aggiunge il metodo
    clean_url = base_url.strip().rstrip('/')
    bman_url = f"{clean_url}/getAnagrafiche"
    
    # Payload per il test
    payload = {
        'chiave': bman_key,
        'filtri': json.dumps([]),
        'numero di pagina': 1
    }

    try:
        # User-Agent aggiunto per evitare blocchi firewall comuni
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.post(bman_url, data=payload, headers=headers, timeout=15)
        
        if response.status_code == 200:
            return "✅ Connessione Bman OK! I dati sono accessibili."
        elif response.status_code == 404:
            return f"❌ Errore 404: L'URL '{bman_url}' non è corretto. Verifica BMAN_BASE_URL su Render."
        else:
            return f"❌ Errore Server Bman: Stato {response.status_code}"
            
    except Exception as e:
        raise Exception(f"❌ Errore di connessione: {str(e)}")
