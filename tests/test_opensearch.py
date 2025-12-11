import json
import os
import signal
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List

import requests

RECEIVED_BULK: List[str] = []


class BulkHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/_bulk":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8", errors="ignore")
        RECEIVED_BULK.append(body)
        # Minimal bulk success response
        resp = {"took": 1, "errors": False, "items": []}
        data = json.dumps(resp).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        return


def start_mock_bulk():
    server = HTTPServer(("127.0.0.1", 19999), BulkHandler)
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
        ["./target/debug/mini_vector", "tests/test_opensearch.yml"],
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


def send_logs():
    session = requests.Session()
    url = "http://127.0.0.1:19001/logs"
    # Send JSON array
    arr = [{"msg": "a1"}, {"msg": "a2"}]
    r = session.post(url, json=arr)
    r.raise_for_status()
    # Send NDJSON
    ndjson = "\n".join([json.dumps({"msg": "n1"}), json.dumps({"msg": "n2"}), ""])
    headers = {"Content-Type": "application/x-ndjson"}
    r = session.post(url, data=ndjson.encode("utf-8"), headers=headers)
    r.raise_for_status()
    # allow flush
    time.sleep(1.0)


def main():
    server = start_mock_bulk()
    proc = run_mini_vector()
    if not wait_for_health(19100, timeout_sec=10):
        print("mini_vector health check failed")
        stop_mini_vector(proc)
        server.shutdown()
        raise SystemExit(1)

    try:
        send_logs()
    finally:
        stop_mini_vector(proc)
        server.shutdown()

    # Each event yields 1 header + 1 doc line. We sent 4 events -> expect >= 4 lines in total.
    total_events = 4
    if not RECEIVED_BULK:
        print("FAIL: no bulk requests received")
        raise SystemExit(1)

    lines = "\n".join(RECEIVED_BULK).splitlines()
    doc_lines = [ln for ln in lines if ln.strip() and not ln.strip().startswith("{\"index\"") is False]
    # Simpler check: count index headers
    index_headers = [ln for ln in lines if ln.strip().startswith("{\"index\"")]
    if len(index_headers) < total_events:
        print(f"FAIL: expected at least {total_events} docs, got {len(index_headers)}")
        raise SystemExit(1)

    print("OK: OpenSearch bulk mock test passed")
    raise SystemExit(0)


if __name__ == "__main__":
    main()

