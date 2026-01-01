from flask import Flask, jsonify
import threading
import logging

from scripts.sync_anagrafica import sync_articoli

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "ok",
        "service": "SyncFED",
        "message": "Backend attivo"
    })

@app.route("/sync-anagrafica", methods=["POST"])
def sync_anagrafica():
    logging.info("Pulsante 4: avvio sync anagrafica")

    thread = threading.Thread(target=sync_articoli)
    thread.start()

    return jsonify({
        "status": "started",
        "message": "Sincronizzazione anagrafica avviata"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
