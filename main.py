import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from flask import Flask, request, jsonify, render_template_string
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

# ===========================
# App & Logging
# ===========================
app = Flask(__name__)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("fatigue-server")

# ===========================
# Config (VM-friendly)
# ===========================
PORT = int(os.getenv("PORT", "8080"))  # pattern comune su esempi corso: 8080 [1](https://github.com/mmamei/PervasiveCloud/blob/master/Lezione%2013%20-%20PubSub/pubsub_app/main.py)
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")  # su VM di solito è settato
FIRESTORE_DATABASE = os.getenv("FIRESTORE_DATABASE")  # opzionale, tipicamente "(default)" implicito
INGEST_API_KEY = os.getenv("INGEST_API_KEY")  # se valorizzata, richiede header X-API-Key

# ===========================
# Firestore client (ADC)
# ===========================
def get_db() -> firestore.Client:
    """
    Crea (lazy) un client Firestore usando Application Default Credentials.
    Su Compute Engine con service account attaccato, funziona senza key file. [4](https://oneuptime.com/blog/post/2026-02-17-how-to-set-up-a-firestore-database-in-native-mode-using-the-google-cloud-console/view)
    """
    # Cache semplice sul contesto app
    if "FIRESTORE_CLIENT" in app.config:
        return app.config["FIRESTORE_CLIENT"]

    try:
        # Alcune versioni della libreria supportano database=...
        if FIRESTORE_DATABASE:
            db = firestore.Client(project=PROJECT_ID, database=FIRESTORE_DATABASE)  # type: ignore
        else:
            db = firestore.Client(project=PROJECT_ID) if PROJECT_ID else firestore.Client()
    except TypeError:
        # Fallback per versioni più vecchie (senza parametro database)
        db = firestore.Client(project=PROJECT_ID) if PROJECT_ID else firestore.Client()

    app.config["FIRESTORE_CLIENT"] = db
    logger.info("Firestore client initialized (project=%s, db=%s)", PROJECT_ID, FIRESTORE_DATABASE or "(default)")
    return db


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _parse_timestamp_to_ms(ts_raw: Any) -> Optional[int]:
    """
    Accetta timestamp in ms o seconds, normalizza in ms.
    """
    if ts_raw is None:
        return None
    try:
        ts = float(ts_raw)
    except Exception:
        return None

    if ts > 1e12:  # molto probabilmente già ms
        return int(ts)
    return int(ts * 1000)


def _doc_path(db: firestore.Client, user: str, session_id: str, sensor: str, ts_ms: int) -> firestore.DocumentReference:
    """
    Struttura Firestore:
      sensors/{user}/sessions/{session}/sensors/{sensor}/readings/{timestamp_ms}
    """
    return (
        db.collection("sensors").document(user)
          .collection("sessions").document(session_id)
          .collection("sensors").document(sensor)
          .collection("readings").document(str(ts_ms))
    )


def _normalize_payload(data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Payload atteso (come il tuo):
      {
        "user": "01",
        "session": "02",
        "sensor": "ACC",
        "timestamp": "1630411794250",  # opzionale
        "data": {...}                  # richiesto
      }
    """
    for k in ("user", "session", "sensor", "data"):
        if k not in data:
            return None, f"Missing field: {k}"

    user = str(data["user"]).strip()
    session_id = str(data["session"]).strip()
    sensor = str(data["sensor"]).strip()
    values = data["data"]

    if not user or not session_id or not sensor:
        return None, "user/session/sensor must be non-empty strings"
    if not isinstance(values, dict):
        return None, "data must be an object (JSON dict)"

    ts_ms = _parse_timestamp_to_ms(data.get("timestamp")) or _now_ms()

    return {
        "user": user,
        "session": session_id,
        "sensor": sensor,
        "timestamp_ms": ts_ms,
        "values": values,
    }, None


def _check_api_key() -> bool:
    """
    Se INGEST_API_KEY è impostata, richiede header X-API-Key.
    """
    if not INGEST_API_KEY:
        return True
    return request.headers.get("X-API-Key") == INGEST_API_KEY


# ===========================
# Routes
# ===========================
@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


@app.route("/data", methods=["POST"])
def ingest():
    if not _check_api_key():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    body = request.get_json(silent=True)
    if not body:
        return jsonify({"status": "error", "message": "Missing JSON body"}), 400

    normalized, err = _normalize_payload(body)
    if err:
        return jsonify({"status": "error", "message": err}), 400

    db = get_db()

    user = normalized["user"]
    session_id = normalized["session"]
    sensor = normalized["sensor"]
    ts_ms = normalized["timestamp_ms"]
    values = normalized["values"]

    ref = _doc_path(db, user, session_id, sensor, ts_ms)

    doc = {
        "user": user,
        "session": session_id,
        "sensor": sensor,
        "timestamp_ms": ts_ms,
        "values": values,
        "ingested_at": firestore.SERVER_TIMESTAMP,
    }

    try:
        ref.set(doc, merge=True)  # idempotente sul doc_id=timestamp
    except GoogleAPICallError as e:
        logger.exception("Firestore write failed: %s", e)
        return jsonify({"status": "error", "message": "Firestore write failed"}), 500
    except Exception as e:
        logger.exception("Unexpected ingest error: %s", e)
        return jsonify({"status": "error", "message": "Internal error"}), 500

    return jsonify({"status": "ok"}), 200


# ===========================
# Dashboard HTML (semplice)
# ===========================
DASHBOARD_HTML = """
<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <title>FatigueSet – Firestore Live View</title>
  <meta http-equiv="refresh" content="10">
  <style>
    body { font-family: Segoe UI, Arial, sans-serif; background:#f6f7fb; margin:0; padding:20px; }
    .card { background:white; border-radius:12px; padding:16px; box-shadow:0 6px 18px rgba(0,0,0,.06); }
    h1 { margin:0 0 10px; color:#1a73e8; }
    .row { display:flex; gap:12px; flex-wrap:wrap; margin:12px 0; align-items:center; }
    input { padding:10px; border:1px solid #d0d7de; border-radius:8px; }
    button { padding:10px 14px; border:0; border-radius:8px; background:#1a73e8; color:white; cursor:pointer; }
    .muted { color:#666; font-size:.9em; }
    pre { white-space: pre-wrap; word-wrap: break-word; background:#0b1020; color:#e6edf3; padding:12px; border-radius:10px; overflow:auto; }
    a { color:#1a73e8; text-decoration:none; }
    code { background:#eef2ff; padding:2px 6px; border-radius:6px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>✅ Dati salvati su Firestore</h1>
    <div class="muted">
      Auto-refresh ogni 10s. Filtri opzionali: <code>user</code>, <code>session</code>, <code>sensor</code>.
      API JSON: <a href="/api/latest">/api/latest</a>
    </div>

    <form class="row" method="get" action="/">
      <input name="user" placeholder="user (es. 01)" value="{{ user or '' }}">
      <input name="session" placeholder="session (es. 02)" value="{{ session or '' }}">
      <input name="sensor" placeholder="sensor (es. ACC)" value="{{ sensor or '' }}">
      <input name="limit" placeholder="limit (default 50)" value="{{ limit or '' }}">
      <button type="submit">Applica</button>
    </form>

    <h3>Ultime letture</h3>
    <pre>{{ lines }}</pre>
  </div>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def dashboard():
    user = request.args.get("user") or None
    session_id = request.args.get("session") or None
    sensor = request.args.get("sensor") or None
    limit = int(request.args.get("limit") or 50)
    limit = max(1, min(limit, 500))

    db = get_db()

    q = (
        db.collection_group("readings")
          .order_by("timestamp_ms", direction=firestore.Query.DESCENDING)
          .limit(limit)
    )
    if user:
        q = q.where("user", "==", user)
    if session_id:
        q = q.where("session", "==", session_id)
    if sensor:
        q = q.where("sensor", "==", sensor)

    try:
        docs = q.stream()
    except Exception as e:
        logger.exception("Firestore query failed (maybe missing index): %s", e)
        return render_template_string(
            DASHBOARD_HTML,
            user=user, session=session_id, sensor=sensor, limit=limit,
            lines="Errore query Firestore (possibile indice mancante). Controlla i log."
        )

    lines = []
    for d in docs:
        x = d.to_dict() or {}
        ts = x.get("timestamp_ms")
        try:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            dt = "N/A"
        lines.append(
            f'USER:{x.get("user")} | SESSION:{x.get("session")} | SENSOR:{x.get("sensor")} | {dt} | values={x.get("values")}'
        )

    return render_template_string(
        DASHBOARD_HTML,
        user=user, session=session_id, sensor=sensor, limit=limit,
        lines="\n".join(lines) if lines else "(Nessun dato trovato)"
    )


@app.route("/api/latest", methods=["GET"])
def api_latest():
    limit = int(request.args.get("limit") or 50)
    limit = max(1, min(limit, 500))
    db = get_db()
    q = (
        db.collection_group("readings")
          .order_by("timestamp_ms", direction=firestore.Query.DESCENDING)
          .limit(limit)
    )
    docs = [d.to_dict() for d in q.stream()]
    return jsonify(docs), 200


if __name__ == "__main__":
    # Su VM: esponi su 0.0.0.0 e porta 8080 (pattern corso) [1](https://github.com/mmamei/PervasiveCloud/blob/master/Lezione%2013%20-%20PubSub/pubsub_app/main.py)
    app.run(host="0.0.0.0", port=PORT, debug=False)