import os
import requests
import json

def run():
    # Recupero variabili da Render
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    
    if not bman_key or not base_url:
        raise Exception("Variabili BMAN_API_KEY o BMAN_BASE_URL mancanti su Render")

    bman_url = f"{base_url}/getAnagrafiche"
    
    # Payload minimo per testare la connessione
    payload = {
        'chiave': bman_key,
        'filtri': json.dumps([]),
        'numero di pagina': 1,
        'quantità per pagina': 1 # Chiediamo solo 1 record per velocità
    }

    try:
        response = requests.post(bman_url, data=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # Se Bman risponde con un messaggio di errore nel JSON
            if isinstance(data, dict) and data.get("status") == "error":
                return f"❌ Errore Bman: {data.get('message', 'Chiave non valida')}"
            
            return "✅ Connessione Bman OK! Dati accessibili correttamente."
        else:
            return f"❌ Errore Server Bman: Stato {response.status_code}"
            
    except Exception as e:
        raise Exception(f"❌ Errore di connessione: {str(e)}")
