"""Kobo highlights sync server.

Accepts POSTed highlight JSON from a Kobo e-reader and (eventually) writes
highlights into Calibre's metadata.db.  This module implements the HTTP
skeleton only — database logic is added in a later task.

Usage:
    python server.py [--port 8787] [--db /path/to/metadata.db] [--verbose]
"""

import argparse
import json
import logging
import sqlite3
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer

from matcher import match_book
from writer import kobo_to_calibre_annotation, write_annotations

logger = logging.getLogger(__name__)


class _SyncHandler(BaseHTTPRequestHandler):
    """Request handler for the sync server."""

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._handle_health()
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path == "/sync":
            self._handle_sync()
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    # Reject wrong methods on known paths
    def do_DELETE(self):  # noqa: N802
        self._method_not_allowed()

    def do_PUT(self):  # noqa: N802
        self._method_not_allowed()

    def do_PATCH(self):  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self):
        if self.path in ("/health", "/sync"):
            self._respond(405, {"status": "error", "message": "method not allowed"})
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_health(self):
        self._respond(200, {"status": "ok"})

    def _handle_sync(self):
        # Read body
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length > 0 else b""

        if not raw:
            self._respond(400, {"status": "error", "message": "empty body"})
            return

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            self._respond(400, {"status": "error", "message": f"invalid JSON: {exc}"})
            return

        if "device_id" not in payload:
            self._respond(400, {"status": "error", "message": "missing device_id"})
            return

        if "highlights" not in payload:
            self._respond(400, {"status": "error", "message": "missing highlights"})
            return

        highlights = payload["highlights"]
        count = len(highlights)
        device_id = payload["device_id"]

        logger.info(
            "Received %d highlight(s) from device '%s'", count, device_id
        )

        # Group highlights by (book_title, book_author, book_isbn)
        groups: dict = defaultdict(list)
        for hl in highlights:
            key = (
                hl.get("book_title", ""),
                hl.get("book_author", ""),
                hl.get("book_isbn", ""),
            )
            groups[key].append(hl)

        results = []
        unmatched = []

        conn = sqlite3.connect(self.server.db_path)
        try:
            for (book_title, book_author, book_isbn), group in groups.items():
                book_id = match_book(
                    conn,
                    isbn=book_isbn or None,
                    title=book_title or None,
                    author=book_author or None,
                )
                if book_id is None:
                    unmatched.append({
                        "title": book_title,
                        "author": book_author,
                        "count": len(group),
                    })
                    logger.warning(
                        "No Calibre match for book '%s' by '%s'", book_title, book_author
                    )
                else:
                    annotations = [kobo_to_calibre_annotation(hl) for hl in group]
                    stats = write_annotations(conn, book_id, annotations)
                    results.append({
                        "book_title": book_title,
                        "book_id": book_id,
                        "inserted": stats["inserted"],
                        "skipped": stats["skipped"],
                    })
                    logger.info(
                        "Book '%s' (id=%d): inserted=%d skipped=%d",
                        book_title, book_id, stats["inserted"], stats["skipped"],
                    )
        finally:
            conn.close()

        self._respond(200, {
            "status": "ok",
            "received": count,
            "results": results,
            "unmatched": unmatched,
        })

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _respond(self, code: int, body: dict):
        encoded = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt, *args):  # silence default stderr logging
        logger.debug("%s - %s", self.address_string(), fmt % args)

    # Wrong-method handling for /health (GET only) and /sync (POST only)
    # We need a special case: POST to /health and GET to /sync → 405
    def do_HEAD(self):  # noqa: N802
        if self.path in ("/health", "/sync"):
            self._respond(405, {"status": "error", "message": "method not allowed"})
        else:
            self._respond(404, {"status": "error", "message": "not found"})


# Patch the handler so that POST /health and GET /sync return 405
_original_get = _SyncHandler.do_GET
_original_post = _SyncHandler.do_POST


def _patched_get(self):
    if self.path == "/sync":
        self._respond(405, {"status": "error", "message": "method not allowed"})
    else:
        _original_get(self)


def _patched_post(self):
    if self.path == "/health":
        self._respond(405, {"status": "error", "message": "method not allowed"})
    else:
        _original_post(self)


_SyncHandler.do_GET = _patched_get
_SyncHandler.do_POST = _patched_post


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = "/mnt/usb/t7-1/media/books/metadata.db"


def create_server(port: int = 8787, db_path: str = DEFAULT_DB_PATH) -> HTTPServer:
    """Create and return a configured HTTPServer (not yet started).

    Args:
        port:    TCP port to bind.
        db_path: Path to Calibre's metadata.db (stored for later use).

    Returns:
        An HTTPServer instance ready for serve_forever() or handle_request().
    """
    server = HTTPServer(("", port), _SyncHandler)
    server.db_path = db_path  # stash for handler access via self.server.db_path
    logger.debug("Created server on port %d (db_path=%s)", port, db_path)
    return server


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Kobo highlights sync server")
    parser.add_argument("--port", type=int, default=8787, help="TCP port (default: 8787)")
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        metavar="PATH",
        help="Path to Calibre metadata.db",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    server = create_server(port=args.port, db_path=args.db)
    logger.info("Listening on port %d (db=%s)", args.port, args.db)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
