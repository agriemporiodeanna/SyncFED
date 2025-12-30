import os
from flask import Flask, send_from_directory, jsonify
import traceback
import scripts.sync_articoli as sync_articoli
import scripts.test_google as test_google
import scripts.test_bman as test_bman

app = Flask(__name__)

@app.route('/dashboard.html')
def dashboard():
    return send_from_directory('static', 'dashboard.html')

@app.route('/api/<action>', methods=['POST'])
def handle_api(action):
    try:
        if action == 'test-google':
            msg = test_google.run()
        elif action == 'test-bman':
            msg = test_bman.run()
        elif action == 'sync-articoli':
            msg = sync_articoli.run()
        else:
            return jsonify({"status": "error", "message": "Azione non valida"}), 404
        
        return jsonify({"status": "success", "message": msg})
    
    except Exception as e:
        # Recupera l'errore completo con i dettagli di Google
        full_error = traceback.format_exc()
        print(f"ERRORE DETTAGLIATO:\n{full_error}")
        return jsonify({"status": "error", "message": f"Errore: {str(e)}", "details": full_error}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
