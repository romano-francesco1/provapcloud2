import argparse
import csv
import os
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import requests

# filename : (sensor_name, interval_seconds)
SENSOR_CONFIGS: Dict[str, Tuple[str, float]] = {
    "wrist_acc.csv": ("ACC", 1/32),   # 32 Hz
    "wrist_bvp.csv": ("BVP", 1/64),   # 64 Hz
    "wrist_eda.csv": ("EDA", 1/4),    # 4 Hz
    "wrist_hr.csv":  ("HR", 1.0),     # 1 Hz
    "wrist_ibi.csv": ("IBI", 0.8),    # simulazione
    "wrist_skin_temperature.csv": ("TEMP", 1/4),  # 4 Hz
}

SCAN_EVERY_SECONDS_DEFAULT = 5


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _to_number_if_possible(v: Any) -> Any:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        # int se possibile, altrimenti float
        if s.isdigit():
            return int(s)
        return float(s)
    except Exception:
        return v


class FatigueSetClientManager:
    """
    - più sessioni/utenti contemporanei
    - inserimento dinamico di nuove cartelle durante l'esecuzione
    """
    def __init__(self, server_url: str, timeout_s: int = 5):
        self.server_url = server_url.rstrip("/")
        self.started_roots = set()
        self.timeout_s = timeout_s
        self.http = requests.Session()

    def _infer_user_session(self, file_path: str):
        parts = file_path.split(os.sep)
        # Atteso: .../{user}/{session}/wrist_xxx.csv
        user = parts[-3] if len(parts) >= 3 else "unknown_user"
        session = parts[-2] if len(parts) >= 2 else "unknown_session"
        return user, session

    def _send_file_stream(self, file_path: str, sensor_name: str, interval: float):
        user, session = self._infer_user_session(file_path)
        print(f"[START] user={user} session={session} sensor={sensor_name} file={file_path}")

        try:
            with open(file_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = row.pop("timestamp", None)
                    if ts is None:
                        ts = _now_ms()

                    payload = {
                        "user": user,
                        "session": session,
                        "sensor": sensor_name,
                        "timestamp": ts,
                        "data": {k: _to_number_if_possible(v) for k, v in row.items()},
                    }

                    try:
                        r = self.http.post(self.server_url, json=payload, timeout=self.timeout_s)
                        if r.status_code != 200:
                            print(f"[WARN] HTTP {r.status_code}: {r.text[:200]}")
                    except Exception as e:
                        print(f"[WARN] POST failed: {e}")

                    time.sleep(interval)

        except FileNotFoundError:
            print(f"[ERR] file non trovato: {file_path}")
        except Exception as e:
            print(f"[ERR] errore stream {file_path}: {e}")

    def monitor_directory(self, base_dir: str, scan_every_seconds: int = SCAN_EVERY_SECONDS_DEFAULT):
        base_dir = os.path.abspath(base_dir)
        print(f"[MONITOR] in ascolto su: {base_dir}")

        while True:
            for root, _, files in os.walk(base_dir):
                if any(fn in SENSOR_CONFIGS for fn in files):
                    if root not in self.started_roots:
                        self.started_roots.add(root)
                        print(f"\n[NEW SOURCE] {root}")

                        for fn in files:
                            if fn in SENSOR_CONFIGS:
                                sensor_name, interval = SENSOR_CONFIGS[fn]
                                full_path = os.path.join(root, fn)
                                t = threading.Thread(
                                    target=self._send_file_stream,
                                    args=(full_path, sensor_name, interval),
                                    daemon=True
                                )
                                t.start()

            time.sleep(scan_every_seconds)


def main():
    parser = argparse.ArgumentParser(description="FatigueSet IoT client - stream CSV -> POST /data")
    parser.add_argument("--base", default="fatigueset", help="Directory base dataset (default: ./fatigueset)")
    parser.add_argument("--server-url", required=True,
                        help="URL completo endpoint /data (es: https://<PROJECT_ID>.<REGION>.r.appspot.com/data)")
    parser.add_argument("--scan-every", type=int, default=5, help="Secondi tra scansioni directory (default 5)")
    parser.add_argument("--timeout", type=int, default=5, help="Timeout HTTP (default 5s)")
    args = parser.parse_args()

    mgr = FatigueSetClientManager(args.server_url, timeout_s=args.timeout)
    try:
        mgr.monitor_directory(args.base, scan_every_seconds=args.scan_every)
    except KeyboardInterrupt:
        print("\n[STOP] interrotto dall'utente.")
        sys.exit(0)


if __name__ == "__main__":
    main()