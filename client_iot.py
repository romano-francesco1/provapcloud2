import argparse
import csv
import os
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# filename : (sensor_name, interval_seconds)
SENSOR_CONFIGS: Dict[str, Tuple[str, float]] = {
    "wrist_acc.csv": ("ACC", 1/32),   # 32 Hz
    "wrist_bvp.csv": ("BVP", 1/64),   # 64 Hz
    "wrist_eda.csv": ("EDA", 1/4),    # 4 Hz
    "wrist_hr.csv":  ("HR", 1.0),     # 1 Hz
    "wrist_ibi.csv": ("IBI", 0.8),    # simulazione (dataset reale è per-battito)
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
        if s.isdigit():
            return int(s)
        return float(s)
    except Exception:
        return v


def _build_session(timeout_s: int, retries: int, backoff: float) -> requests.Session:
    """
    Session HTTP con retry/backoff: utile quando la VM riavvia o la rete è instabile.
    """
    sess = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST", "GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.request = _wrap_timeout(sess.request, timeout_s)
    return sess


def _wrap_timeout(request_func, timeout_s: int):
    def _wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout_s
        return request_func(method, url, **kwargs)
    return _wrapped


class FatigueSetClientManager:
    """
    - più sessioni/utenti contemporanei
    - inserimento dinamico di nuove cartelle durante l'esecuzione
    """

    def __init__(
        self,
        server_base: str,
        endpoint: str = "/data",
        timeout_s: int = 5,
        api_key: Optional[str] = None,
        retries: int = 3,
        backoff: float = 0.5,
        speed: float = 1.0,
        max_lines: Optional[int] = None
    ):
        # stile "base_url + path" come negli esempi del repo (client -> POST) [2](https://github.com/mmamei/PervasiveCloud/blob/master/Aula/esempio2/client_sensors.py)
        self.server_base = server_base.rstrip("/")
        self.endpoint = endpoint if endpoint.startswith("/") else "/" + endpoint
        self.url = self.server_base + self.endpoint

        self.started_roots = set()
        self.timeout_s = timeout_s
        self.api_key = api_key
        self.speed = max(0.001, float(speed))
        self.max_lines = max_lines

        self.http = _build_session(timeout_s=timeout_s, retries=retries, backoff=backoff)

    def _infer_user_session(self, file_path: str):
        parts = file_path.split(os.sep)
        # Atteso: .../{user}/{session}/wrist_xxx.csv
        user = parts[-3] if len(parts) >= 3 else "unknown_user"
        session = parts[-2] if len(parts) >= 2 else "unknown_session"
        return user, session

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    def _send_file_stream(self, file_path: str, sensor_name: str, interval: float):
        user, session = self._infer_user_session(file_path)
        print(f"[START] user={user} session={session} sensor={sensor_name} file={file_path}")

        sent = 0
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
                        r = self.http.post(self.url, json=payload, headers=self._headers())
                        if r.status_code != 200:
                            print(f"[WARN] HTTP {r.status_code}: {r.text[:200]}")
                    except Exception as e:
                        print(f"[WARN] POST failed: {e}")

                    sent += 1
                    if self.max_lines and sent >= self.max_lines:
                        print(f"[INFO] max_lines raggiunto ({self.max_lines}) per {file_path}")
                        break

                    # speed > 1 => più veloce (sleep più corto)
                    time.sleep(interval / self.speed)

        except FileNotFoundError:
            print(f"[ERR] file non trovato: {file_path}")
        except Exception as e:
            print(f"[ERR] errore stream {file_path}: {e}")

    def monitor_directory(self, base_dir: str, scan_every_seconds: int = SCAN_EVERY_SECONDS_DEFAULT):
        base_dir = os.path.abspath(base_dir)
        print(f"[MONITOR] base={base_dir}")
        print(f"[TARGET] url={self.url}")

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

    # invece di chiedere URL completo, chiediamo base + endpoint (più comodo su VM)
    parser.add_argument("--server-base", required=True, help="Base URL server (es: http://<VM_EXTERNAL_IP>)")
    parser.add_argument("--endpoint", default="/data", help="Endpoint (default: /data)")

    parser.add_argument("--scan-every", type=int, default=5, help="Secondi tra scansioni directory (default 5)")
    parser.add_argument("--timeout", type=int, default=5, help="Timeout HTTP (default 5s)")
    parser.add_argument("--retries", type=int, default=3, help="Numero retry HTTP (default 3)")
    parser.add_argument("--backoff", type=float, default=0.5, help="Backoff factor retry (default 0.5)")
    parser.add_argument("--api-key", default=None, help="API key (header X-API-Key) se server la richiede")

    parser.add_argument("--speed", type=float, default=1.0,
                        help="Fattore velocità simulazione. 2.0 = 2x più veloce (sleep dimezzato)")
    parser.add_argument("--max-lines", type=int, default=None,
                        help="Max righe da inviare per file (utile per demo/test)")

    args = parser.parse_args()

    mgr = FatigueSetClientManager(
        server_base=args.server_base,
        endpoint=args.endpoint,
        timeout_s=args.timeout,
        api_key=args.api_key,
        retries=args.retries,
        backoff=args.backoff,
        speed=args.speed,
        max_lines=args.max_lines
    )

    try:
        mgr.monitor_directory(args.base, scan_every_seconds=args.scan_every)
    except KeyboardInterrupt:
        print("\n[STOP] interrotto dall'utente.")
        sys.exit(0)


if __name__ == "__main__":
    main()