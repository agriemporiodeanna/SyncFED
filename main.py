from flask import Flask, jsonify, send_from_directory
import threading

from scripts import test_bman, test_google, sync_articoli, sync_anagrafica

app = Flask(__name__, static_folder="static")


def _as_json(result, default_ok_message="OK"):
    """
    Uniforma le risposte:
    - se lo script torna dict -> lo passa
    - se torna string -> wrap in {status,message}
    """
    if isinstance(result, dict):
        return result
    return {"status": "ok", "message": str(result) if result is not None else default_ok_message}


def _run_in_thread(fn):
    t = threading.Thread(target=fn, daemon=True)
    t.start()


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "service": "SyncFED", "message": "Backend attivo"})


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return send_from_directory(app.static_folder, "dashboard.html")


# PULSANTE 1
@app.route("/test-bman", methods=["POST"])
def route_test_bman():
    try:
        res = test_bman.run()
        return jsonify(_as_json(res, "Connessione Bman OK"))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# PULSANTE 2
@app.route("/test-google", methods=["POST"])
def route_test_google():
    try:
        res = test_google.run()
        return jsonify(_as_json(res, "Connessione Google Sheet OK"))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# PULSANTE 3
@app.route("/sync-articoli", methods=["POST"])
def route_sync_articoli():
    def job():
        try:
            sync_articoli.run()
        except Exception:
            pass

    _run_in_thread(job)
    return jsonify({"status": "started", "message": "Export articoli avviato"})


# PULSANTE 4
@app.route("/sync-anagrafica", methods=["POST"])
def route_sync_anagrafica():
    def job():
        try:
            sync_anagrafica.run()
        except Exception:
            pass

    _run_in_thread(job)
    return jsonify({"status": "started", "message": "Sync anagrafica avviata"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)


