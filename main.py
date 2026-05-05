import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from flask import Flask, request, jsonify, render_template_string
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

app = Flask(__name__)

# Logging utile sia in Cloud Shell che su App Engine (Log Explorer)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("pcloud")

# Nome progetto (utile per debug)
app.config["PROJECT_NAME"] = os.getenv("GOOGLE_CLOUD_PROJECT", "local")

# Firestore client (ADC in Cloud Shell / service account in App Engine)
db = firestore.Client()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _parse_timestamp_to_ms(ts_raw: Any) -> Optional[int]:
    """
    FatigueSet tipicamente ha timestamp in millisecondi.
    Accettiamo anche secondi (o stringhe), e normalizziamo in ms.
    """
    if ts_raw is None:
        return None
    try:
        ts = float(ts_raw)
    except Exception:
        return None

    # Se > 1e12 probabilmente sono ms (es. 1630411794250)
    # Se ~1e9 probabilmente sono seconds
    if ts > 1e12:
        return int(ts)
    return int(ts * 1000)


def _doc_path(user: str, session_id: str, sensor: str, timestamp_ms_str: str) -> firestore.DocumentReference:
    """
    Struttura Firestore:
      sensors/{user}/sessions/{session}/sensors/{sensor}/readings/{timestamp_ms}
    """
    return (
        db.collection("sensors").document(user)
          .collection("sessions").document(session_id)
          .collection("sensors").document(sensor)
          .collection("readings").document(timestamp_ms_str)
    )


def _normalize_payload(data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Payload atteso:
      {
        "user": "01",
        "session": "02",
        "sensor": "ACC",
        "timestamp": "1630411794250",   # opzionale
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

    ts_ms = _parse_timestamp_to_ms(data.get("timestamp"))
    if ts_ms is None:
        ts_ms = _now_ms()

    return {
        "user": user,
        "session": session_id,
        "sensor": sensor,
        "timestamp_ms": ts_ms,
        "values": values,
    }, None


# ===========================
# Ingest API
# ===========================
@app.route("/data", methods=["POST"])
def ingest():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"status": "error", "message": "Missing JSON body"}), 400

    normalized, err = _normalize_payload(body)
    if err:
        return jsonify({"status": "error", "message": err}), 400

    user = normalized["user"]
    session_id = normalized["session"]
    sensor = normalized["sensor"]
    ts_ms = normalized["timestamp_ms"]
    values = normalized["values"]

    ref = _doc_path(user, session_id, sensor, str(ts_ms))

    doc = {
        "user": user,
        "session": session_id,
        "sensor": sensor,
        "timestamp_ms": ts_ms,
        "values": values,
        # timestamp robusto server-side
        "ingested_at": firestore.SERVER_TIMESTAMP,
    }

    try:
        # merge=True -> idempotente se arriva lo stesso doc_id
        ref.set(doc, merge=True)
    except GoogleAPICallError as e:
        logger.exception("Firestore write failed: %s", e)
        return jsonify({"status": "error", "message": "Firestore write failed"}), 500
    except Exception as e:
        logger.exception("Unexpected ingest error: %s", e)
        return jsonify({"status": "error", "message": "Internal error"}), 500

    return jsonify({"status": "ok"}), 200


# ===========================
# Dashboard HTML
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
            lines="Errore query Firestore (possibile indice mancante o permessi). Controlla i log."
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


# ===========================
# API JSON
# ===========================
@app.route("/api/latest", methods=["GET"])
def api_latest():
    limit = int(request.args.get("limit") or 50)
    limit = max(1, min(limit, 500))
    q = (
        db.collection_group("readings")
          .order_by("timestamp_ms", direction=firestore.Query.DESCENDING)
          .limit(limit)
    )
    docs = [d.to_dict() for d in q.stream()]
    return jsonify(docs), 200


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


if __name__ == "__main__":
    # Locale / Cloud Shell: python main.py
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)