import http.server
import threading
import time
import requests
import subprocess
import json
import os
import signal
import sys

# Mock Sink Server
RECEIVED_EVENTS = []

class MockHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/recv":
            length = int(self.headers['Content-Length'])
            data = self.rfile.read(length)
            try:
                batch = json.loads(data)
                if isinstance(batch, list):
                    RECEIVED_EVENTS.extend(batch)
                else:
                    RECEIVED_EVENTS.append(batch)
                self.send_response(200)
            except Exception as e:
                print(f"Server Error: {e}")
                self.send_response(400)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass # Silence server logs

def run_server():
    server = http.server.HTTPServer(('127.0.0.1', 19002), MockHandler)
    server.serve_forever()

server_thread = threading.Thread(target=run_server, daemon=True)
server_thread.start()

print("Started Mock Sink on 19002", flush=True)

# Start Mini Vector
env = os.environ.copy()
env["RUST_LOG"] = "info"
env["METRICS_PORT"] = "19100"
print("Compiling and starting Mini Vector...", flush=True)

# Use setsid to create a new process group so we can kill the whole tree
proc = subprocess.Popen(
    ["./target/debug/mini_vector", "tests/test_config.yml"],
    env=env,
    preexec_fn=os.setsid,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

# Wait a bit for startup (cargo build might take time)
# We could poll the health endpoint
time.sleep(1) # Give it a second to start spawning
print("Waiting for Mini Vector to be ready...", flush=True)

health_url = "http://127.0.0.1:19100/health"
ready = False
for _ in range(30):
    try:
        r = requests.get(health_url)
        if r.status_code == 200:
            ready = True
            print("Mini Vector is ready!", flush=True)
            break
    except requests.exceptions.ConnectionError:
        pass
    time.sleep(1)

if not ready:
    print("Mini Vector failed to start.", flush=True)
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    print("STDERR:", flush=True)
    print(proc.stderr.read().decode(), flush=True)
    sys.exit(1)

# Send logs
print("Sending 1000 logs...", flush=True)
session = requests.Session()
url = "http://127.0.0.1:19001/logs"

start_time = time.time()
for i in range(1000):
    payload = {"msg": f"log_{i}", "count": i}
    try:
        r = session.post(url, json=payload)
        if r.status_code != 200:
            print(f"Failed to send log {i}: status {r.status_code}", flush=True)
    except Exception as e:
        print(f"Failed to send log {i}: {e}", flush=True)

duration = time.time() - start_time
print(f"Sent 1000 logs in {duration:.2f}s", flush=True)

# Wait for batching flushes (max timeout 1s)
time.sleep(2)

count = len(RECEIVED_EVENTS)
print(f"Received {count} events", flush=True)

# Shutdown
print("Shutting down Mini Vector...", flush=True)
os.killpg(os.getpgid(proc.pid), signal.SIGINT) # SIGINT for graceful shutdown
try:
    proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    print("Mini Vector timed out on shutdown, killing...", flush=True)
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)

# Print logs
print("Mini Vector Output:", flush=True)
print(proc.stdout.read().decode(), flush=True)
print("Mini Vector Stderr:", flush=True)
print(proc.stderr.read().decode(), flush=True)

if count == 1000:
    print("SUCCESS: Received all 1000 logs", flush=True)
    sys.exit(0)
else:
    print(f"FAILURE: Expected 1000, got {count}", flush=True)
    sys.exit(1)
