import os
import requests
import json

def run():
    bman_key = os.environ.get("BMAN_API_KEY")
    base_url = os.environ.get("BMAN_BASE_URL")
    
    if not bman_key or not base_url:
        raise Exception("Variabili BMAN_API_KEY o BMAN_BASE_URL mancanti su Render.")

    clean_url = base_url.strip().rstrip('/')
    # Il metodo corretto per getAnagrafiche
    bman_url = f"{clean_url}/getAnagrafiche"
    
    # Payload completo come richiesto dai parametri della funzione getAnagrafiche
    payload = {
        'chiave': bman_key,
        'filtri': json.dumps([]),
        'ordinamentoCampo': 'ID',
        'ordinamentoDirezione': 1, # 1 - ASC
        'numero di pagina': 1, # Nota gli spazi nel nome del parametro
        'listaDepositi': '',
        'dettaglioVarianti': 'False'
    }

    try:
        # Usiamo data=payload per inviare come application/x-www-form-urlencoded
        response = requests.post(bman_url, data=payload, timeout=15)
        
        if response.status_code == 200:
            return "✅ Connessione Bman OK! Il server ha risposto correttamente."
        else:
            # In caso di errore 500, stampiamo parte della risposta per debugging
            return f"❌ Errore Bman (Stato {response.status_code}): Il server non ha accettato i parametri."
            
    except Exception as e:
        raise Exception(f"❌ Errore tecnico: {str(e)}")
