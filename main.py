import os
from flask import Flask, send_from_directory, jsonify
import scripts.sync_articoli as sync_articoli

app = Flask(__name__)

@app.route('/dashboard.html')
def dashboard():
    return send_from_directory('static', 'dashboard.html')

@app.route('/api/sync-articoli', methods=['POST'])
def trigger_articoli():
    try:
        messaggio = sync_articoli.run()
        return jsonify({"status": "success", "message": messaggio})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Usa la porta configurata su Render
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
