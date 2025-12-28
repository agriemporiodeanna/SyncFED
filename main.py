import os
from flask import Flask, send_from_directory, jsonify
import scripts.sync_articoli as sync_articoli
import scripts.test_google as test_google
import scripts.test_bman as test_bman

app = Flask(__name__)

@app.route('/dashboard.html')
def dashboard():
    return send_from_directory('static', 'dashboard.html')

# 1. Test Google
@app.route('/api/test-google', methods=['POST'])
def trigger_test_google():
    try:
        messaggio = test_google.run()
        return jsonify({"status": "success", "message": messaggio})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 2. Test Bman
@app.route('/api/test-bman', methods=['POST'])
def trigger_test_bman():
    try:
        messaggio = test_bman.run()
        return jsonify({"status": "success", "message": messaggio})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 3. Sincronizzazione Totale
@app.route('/api/sync-articoli', methods=['POST'])
def trigger_articoli():
    try:
        messaggio = sync_articoli.run()
        return jsonify({"status": "success", "message": messaggio})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
