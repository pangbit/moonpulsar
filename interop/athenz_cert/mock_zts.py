"""Disposable HTTPS ZTS fixture that requires a CA-signed client certificate."""

import argparse
import http.server
import json
from pathlib import Path
import ssl
import time


KEYS = Path(__file__).resolve().parents[2] / "testdata" / "athenz_cert"


class Handler(http.server.BaseHTTPRequestHandler):
    failures_left = 0

    def do_GET(self):
        if not self.path.startswith("/zts/v1/domain/provider/token?"):
            self.send_error(404)
            return
        if not self.connection.getpeercert():
            self.send_error(401)
            return
        if Handler.failures_left:
            Handler.failures_left -= 1
            self.send_error(503)
            return
        body = json.dumps(
            {"token": "certificate-role", "expiryTime": int(time.time()) + 3600}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-first", action="store_true")
    args = parser.parse_args()
    Handler.failures_left = int(args.fail_first)
    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(KEYS / "server.crt", KEYS / "server.key")
    tls.load_verify_locations(cafile=KEYS / "ca.crt")
    tls.verify_mode = ssl.CERT_REQUIRED
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    print(f"https://localhost:{server.server_port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
