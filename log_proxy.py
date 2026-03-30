#!/usr/bin/env python3
"""Capture proxy for Kobo reading services API.

Responds to checkforchanges with "changed" for all books,
which should prompt the Kobo to send actual annotation data.
Logs everything to stdout and /tmp/kobo-sync-capture.log.
"""

import json
import logging
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

LOG_FILE = "/tmp/kobo-sync-capture.log"
logging.basicConfig(level=logging.DEBUG, format="%(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)


class CaptureHandler(BaseHTTPRequestHandler):
    def _log_request(self, body=""):
        timestamp = datetime.now().isoformat()
        entry = {
            "timestamp": timestamp,
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers),
            "body_length": len(body) if body else 0,
        }
        if body:
            try:
                entry["body"] = json.loads(body)
            except json.JSONDecodeError:
                entry["body_raw"] = body[:5000]

        formatted = json.dumps(entry, indent=2, default=str)
        log.info("=" * 60)
        log.info(formatted)

        with open(LOG_FILE, "a") as f:
            f.write(formatted + "\n\n")

        return entry

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length > 0:
            return self.rfile.read(length).decode("utf-8", errors="replace")
        return ""

    def _respond(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = self._read_body()
        entry = self._log_request(body)

        if "/checkforchanges" in self.path:
            try:
                books = json.loads(body)
                # Return all ContentIds as changed to trigger full annotation exchange
                changed = [b["ContentId"] for b in books]
                self._respond(200, changed)
                log.info(">>> Responded to checkforchanges: %d books marked as changed", len(changed))
            except Exception as e:
                log.error("Failed to parse checkforchanges: %s", e)
                self._respond(200, [])
        elif "/annotations" in self.path:
            # This is what we're looking for! Log it and accept.
            log.info(">>> ANNOTATIONS RECEIVED!")
            self._respond(200, {"result": "ok"})
        else:
            self._respond(200, {"result": "ok"})

    def do_PATCH(self):
        body = self._read_body()
        self._log_request(body)
        log.info(">>> PATCH request (likely annotations)!")
        self._respond(200, {"result": "ok"})

    def do_GET(self):
        self._log_request()
        if "/annotations" in self.path:
            # Return 200 with empty annotations, NO etag
            # This should tell the Kobo "server is fresh/empty, upload your stuff"
            self._respond(200, {"annotations": [], "nextPageOffsetToken": None})
            log.info(">>> Responded 200 empty annotations (no etag — fresh server)")
        else:
            self._respond(200, {"result": "ok"})

    def do_PUT(self):
        body = self._read_body()
        self._log_request(body)
        self._respond(200, {"result": "ok"})

    def do_DELETE(self):
        body = self._read_body()
        self._log_request(body)
        self._respond(200, {"result": "ok"})

    def log_message(self, fmt, *args):
        log.debug("HTTP: %s", fmt % args)


if __name__ == "__main__":
    port = 8788
    server = HTTPServer(("0.0.0.0", port), CaptureHandler)
    log.info(f"Smart capture proxy listening on port {port}")
    log.info(f"Logging to {LOG_FILE}")
    log.info("Hit sync on your Kobo now...")
    server.serve_forever()
