#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import hashlib
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer


SECRET = os.environ.get("CUBE_WEBHOOK_SECRET", "")


def valid_signature(headers, body):
    if not SECRET:
        return True

    timestamp = headers.get("X-CubeSandbox-Timestamp", "")
    signature = headers.get("X-CubeSandbox-Signature", "")
    expected = hmac.new(
        SECRET.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        if not valid_signature(self.headers, body):
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"invalid signature")
            return

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"invalid json")
            return

        print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
        self.send_response(204)
        self.end_headers()

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", flush=True)


def main():
    host = os.environ.get("CUBE_WEBHOOK_RECEIVER_HOST", "0.0.0.0")
    port = int(os.environ.get("CUBE_WEBHOOK_RECEIVER_PORT", "9000"))
    print(f"listening on http://{host}:{port}/webhook", flush=True)
    HTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
