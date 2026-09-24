"""Disposable ZTS fixture for the isolated Athenz Broker interoperability test."""

import base64
import http.server
import json
from pathlib import Path
import sys
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


directory = Path(sys.argv[1])
service_key = serialization.load_pem_public_key((directory / "service.pub").read_bytes())
role_token = (directory / "role-token").read_text()
expiry = int(next(part[2:] for part in role_token.split(";") if part.startswith("e=")))


def valid_service_token(value):
    try:
        unsigned, signature = value.rsplit(";s=", 1)
        fields = dict(part.split("=", 1) for part in unsigned.split(";"))
        if fields["v"] != "S1" or fields["d"] != "moonpulsar" or fields["n"] != "client":
            return False
        now = int(time.time())
        if int(fields["t"]) > now + 30 or int(fields["e"]) <= now:
            return False
        decoded = base64.b64decode(signature.replace(".", "+").replace("_", "/").replace("-", "="))
        service_key.verify(decoded, unsigned.encode(), padding.PKCS1v15(), hashes.SHA256())
        return True
    except Exception:
        return False


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.path.startswith("/zts/v1/domain/pulsar/token?"):
            self.send_error(404)
            return
        if not valid_service_token(self.headers.get("Athenz-Principal-Auth", "")):
            self.send_error(401)
            return
        body = json.dumps({"token": role_token, "expiryTime": expiry}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
print(f"http://127.0.0.1:{server.server_port}", flush=True)
server.serve_forever()
