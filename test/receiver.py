from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(length)
        print("==== NEW EVENT ====")
        print(body.decode('utf-8', errors='ignore'))
        print("===================")
        self.send_response(200)
        self.end_headers()

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 2021), Handler)
    print("Listening on http://0.0.0.0:2021/logs")
    server.serve_forever()
