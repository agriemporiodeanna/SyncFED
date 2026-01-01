from flask import Flask, jsonify
import threading

from scripts import test_bman, test_google, sync_articoli, sync_anagrafica

app = Flask(__name__)

def threaded(fn):
    thread = threading.Thread(target=fn)
    thread.start()

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "service": "SyncFED"})

@app.route("/test-bman", methods=["POST"])
def route_test_bman():
    return jsonify(test_bman.run())

@app.route("/test-google", methods=["POST"])
def route_test_google():
    return jsonify(test_google.run())

@app.route("/sync-articoli", methods=["POST"])
def route_sync_articoli():
    threaded(sync_articoli.run)
    return jsonify({"status": "started", "message": "Export articoli avviato"})

@app.route("/sync-anagrafica", methods=["POST"])
def route_sync_anagrafica():
    threaded(sync_anagrafica.run)
    return jsonify({"status": "started", "message": "Sync anagrafica avviata"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

