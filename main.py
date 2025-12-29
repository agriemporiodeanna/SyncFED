import os
from flask import Flask, send_from_directory, jsonify
import traceback
import scripts.sync_articoli as sync_articoli
import scripts.test_google as test_google
import scripts.test_bman as test_bman

app = Flask(__name__)

# Serve la dashboard principale
@app.route('/dashboard.html')
def dashboard():
    return send_from_directory('static', 'dashboard.html')

# Rotta API unificata per gestire i pulsanti
@app.route('/api/<action>', methods=['POST'])
def handle_api(action):
    try:
        # 1. Verifica Chiave Google
        if action == 'test-google':
            msg = test_google.run()
        
        # 2. Test Connessione Bman
        elif action == 'test-bman':
            msg = test_bman.run()
        
        # 3. Sincronizzazione Articoli (Filtro opz11 = si/Approvato)
        elif action == 'sync-articoli':
            msg = sync_articoli.run()
            
        else:
            return jsonify({"status": "error", "message": "Azione non riconosciuta"}), 404
        
        # Risposta di successo sempre in formato JSON
        return jsonify({"status": "success", "message": msg})
    
    except Exception as e:
        # Stampa l'errore completo nei log di Render per il debug
        print(f"--- ERRORE RILEVATO NELL'AZIONE {action.upper()} ---")
        traceback.print_exc()
        
        # Restituisce SEMPRE un JSON alla dashboard per evitare errori di parsing JS
        return jsonify({
            "status": "error", 
            "message": f"Errore durante {action}: {str(e)}"
        }), 500

if __name__ == '__main__':
    # Configurazione porta Render
    port = int(os.environ.get("PORT", 10000))
    # Il server Flask risponde all'indirizzo 0.0.0.0 per essere visibile esternamente
    app.run(host='0.0.0.0', port=port)
