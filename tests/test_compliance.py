import gzip
import http.server
import json
import os
import signal
import subprocess
import threading
import time
from typing import List

import requests

# Simple mock sink to collect received events
RECEIVED: List[dict] = []


class MockHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/recv":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
            if isinstance(data, list):
                RECEIVED.extend(data)
            else:
                RECEIVED.append(data)
            self.send_response(200)
        except Exception as e:  # pragma: no cover - diagnostic path
            print(f"mock sink parse error: {e}")
            self.send_response(400)
        self.end_headers()

    def log_message(self, *_):
        return


def start_mock_sink():
    server = http.server.HTTPServer(("127.0.0.1", 19002), MockHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def wait_for_health(port: int, timeout_sec: int = 10) -> bool:
    url = f"http://127.0.0.1:{port}/health"
    for _ in range(timeout_sec * 10):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.1)
    return False


def run_mini_vector():
    env = os.environ.copy()
    env["RUST_LOG"] = "info"
    env["METRICS_PORT"] = "19100"

    proc = subprocess.Popen(
        ["./target/debug/mini_vector", "tests/test_config.yml"],
        env=env,
        preexec_fn=os.setsid,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc


def stop_mini_vector(proc: subprocess.Popen):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def send_array(session: requests.Session):
    url = "http://127.0.0.1:19001/logs"
    payload = [{"msg": "arr1"}, {"msg": "arr2"}]
    r = session.post(url, json=payload)
    r.raise_for_status()


def send_ndjson(session: requests.Session):
    url = "http://127.0.0.1:19001/logs"
    ndjson = '\n'.join([
        json.dumps({"msg": "nd1"}),
        json.dumps({"msg": "nd2"}),
        "",
    ])
    headers = {"Content-Type": "application/x-ndjson"}
    r = session.post(url, data=ndjson.encode("utf-8"), headers=headers)
    r.raise_for_status()


def send_gzip_ndjson(session: requests.Session):
    url = "http://127.0.0.1:19001/logs"
    ndjson = '\n'.join([
        json.dumps({"msg": "gz1"}),
        json.dumps({"msg": "gz2"}),
        "",
    ]).encode("utf-8")
    gz_body = gzip.compress(ndjson)
    headers = {
        "Content-Type": "application/x-ndjson",
        "Content-Encoding": "gzip",
    }
    r = session.post(url, data=gz_body, headers=headers)
    r.raise_for_status()


def main():
    # Start mock sink
    server = start_mock_sink()
    print("Mock sink started on 19002", flush=True)

    # Start mini_vector
    proc = run_mini_vector()
    print("Started mini_vector process", flush=True)

    if not wait_for_health(19100, timeout_sec=10):
        print("mini_vector health check failed", flush=True)
        stop_mini_vector(proc)
        server.shutdown()
        raise SystemExit(1)

    session = requests.Session()
    try:
        send_array(session)
        send_ndjson(session)
        send_gzip_ndjson(session)
        # wait a moment for forwarding
        time.sleep(1.0)
    finally:
        stop_mini_vector(proc)
        server.shutdown()

    expected = 6
    if len(RECEIVED) != expected:
        print(f"FAIL: expected {expected} events, got {len(RECEIVED)}", flush=True)
        raise SystemExit(1)

    print("OK: compliance tests passed", flush=True)
    raise SystemExit(0)


if __name__ == "__main__":
    main()

