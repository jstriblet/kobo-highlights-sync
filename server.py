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
import re
import sqlite3
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from annotation_store import AnnotationStore
from matcher import match_book
from writer import kobo_to_calibre_annotation, write_annotations

logger = logging.getLogger(__name__)

_ANNOTATIONS_RE = re.compile(r"^/api/v3/content/([a-f0-9-]+)/annotations")


class _SyncHandler(BaseHTTPRequestHandler):
    """Request handler for the sync server."""

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def do_GET(self):  # noqa: N802
        parsed_path = urlparse(self.path).path
        if parsed_path == "/sync":
            self._respond(405, {"status": "error", "message": "method not allowed"})
        elif parsed_path == "/health":
            self._handle_health()
        elif parsed_path == "/highlights":
            self._handle_get_highlights()
        else:
            m = _ANNOTATIONS_RE.match(parsed_path)
            if m:
                self._handle_get_annotations(m.group(1))
            else:
                self._respond(404, {"status": "error", "message": "not found"})

    def do_POST(self):  # noqa: N802
        parsed_path = urlparse(self.path).path
        if parsed_path == "/health":
            self._respond(405, {"status": "error", "message": "method not allowed"})
        elif parsed_path == "/sync":
            self._handle_sync()
        elif parsed_path == "/api/v3/content/checkforchanges":
            self._handle_checkforchanges()
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    def do_PATCH(self):  # noqa: N802
        parsed_path = urlparse(self.path).path
        m = _ANNOTATIONS_RE.match(parsed_path)
        if m:
            self._handle_patch_annotations(m.group(1))
        elif urlparse(self.path).path in ("/health", "/sync", "/highlights"):
            self._respond(405, {"status": "error", "message": "method not allowed"})
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    # Reject wrong methods on known paths
    def do_DELETE(self):  # noqa: N802
        self._method_not_allowed()

    def do_PUT(self):  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self):
        if urlparse(self.path).path in ("/health", "/sync", "/highlights"):
            self._respond(405, {"status": "error", "message": "method not allowed"})
        else:
            self._respond(404, {"status": "error", "message": "not found"})

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_health(self):
        self._respond(200, {"status": "ok"})

    def _handle_get_highlights(self):
        params = parse_qs(urlparse(self.path).query)
        book_id = params.get("book_id", [None])[0]

        conn = sqlite3.connect(self.server.db_path)
        try:
            if book_id:
                rows = conn.execute(
                    "SELECT annot_data FROM annotations WHERE book=? AND annot_type='highlight' ORDER BY timestamp",
                    (int(book_id),)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT annot_data FROM annotations WHERE annot_type='highlight' ORDER BY timestamp DESC LIMIT 100"
                ).fetchall()

            highlights = [json.loads(r[0]) for r in rows]
            self._respond(200, {"highlights": highlights, "count": len(highlights)})
        finally:
            conn.close()

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
    # Reading Services API handlers
    # ------------------------------------------------------------------

    def _handle_checkforchanges(self):
        """Respond to Kobo's checkforchanges with changed ContentIds."""
        body = self._read_body()
        if not body:
            self._respond(200, [])
            return
        try:
            books = json.loads(body)
        except json.JSONDecodeError:
            self._respond(200, [])
            return

        store = self.server.annotation_store
        changed = []
        for book in books:
            content_id = book["ContentId"]
            client_etag = book.get("etag", "")
            server_etag = store.get_etag(content_id)
            if client_etag != server_etag:
                changed.append(content_id)

        logger.info("checkforchanges: %d/%d books have changes", len(changed), len(books))
        self._respond(200, changed)

    def _handle_get_annotations(self, content_id):
        """Return stored annotations for a book."""
        store = self.server.annotation_store
        annotations = store.get_annotations(content_id)
        etag = store.get_etag(content_id)

        # Check If-None-Match — if etags match, return 304
        client_etag = self.headers.get("If-None-Match", "")
        if client_etag and client_etag == etag:
            self.send_response(304)
            self.send_header("etag", etag)
            self.end_headers()
            return

        body = json.dumps({"annotations": annotations, "nextPageOffsetToken": None}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("etag", etag)
        self.end_headers()
        self.wfile.write(body)

    def _handle_patch_annotations(self, content_id):
        """Receive annotations from the Kobo and store them."""
        body = self._read_body()
        if not body:
            self._respond(200, {"result": "ok"})
            return

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self._respond(400, {"error": "invalid JSON"})
            return

        annotations = payload.get("updatedAnnotations", [])
        if not annotations:
            self._respond(200, {"result": "ok"})
            return

        store = self.server.annotation_store
        result = store.upsert(content_id, annotations)
        logger.info("PATCH annotations for %s: %d inserted, %d updated",
                    content_id, result["inserted"], result["updated"])

        # Also write highlights to Calibre's metadata.db
        self._write_to_calibre(content_id, annotations)

        # Return new etag
        etag = store.get_etag(content_id)
        body_bytes = json.dumps({"result": "ok"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("etag", etag)
        self.end_headers()
        self.wfile.write(body_bytes)

    def _write_to_calibre(self, content_id, kobo_annotations):
        """Convert Kobo annotations to Calibre format and write to metadata.db."""
        conn = sqlite3.connect(self.server.db_path)
        try:
            # Map content_id (UUID) to Calibre book_id
            row = conn.execute("SELECT id FROM books WHERE uuid=?", (content_id,)).fetchone()
            if not row:
                logger.warning("No Calibre book found for ContentId %s", content_id)
                return
            book_id = row[0]

            # Convert each Kobo annotation to Calibre format
            calibre_annotations = []
            for ann in kobo_annotations:
                if ann.get("type") != "highlight":
                    continue  # Only sync highlights, not dogears

                calibre_ann = kobo_to_calibre_annotation({
                    "bookmark_id": ann["id"],
                    "text": ann.get("highlightedText", ""),
                    "annotation": None,
                    "date_created": ann.get("clientLastModifiedUtc", ""),
                    "chapter_progress": ann.get("location", {}).get("span", {}).get("chapterProgress", 0),
                    "content_id": "",
                    "start_path": ann.get("location", {}).get("span", {}).get("chapterFilename", ""),
                    "end_path": "",
                })
                calibre_annotations.append(calibre_ann)

            if calibre_annotations:
                result = write_annotations(conn, book_id, calibre_annotations)
                logger.info("Wrote to Calibre book %d: %d inserted, %d skipped",
                            book_id, result["inserted"], result["skipped"])
        except Exception as e:
            logger.error("Error writing to Calibre: %s", e)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_body(self):
        """Read and return the request body as a string."""
        length = int(self.headers.get("Content-Length", 0))
        if length > 0:
            return self.rfile.read(length).decode("utf-8", errors="replace")
        return ""

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
        if urlparse(self.path).path in ("/health", "/sync", "/highlights"):
            self._respond(405, {"status": "error", "message": "method not allowed"})
        else:
            self._respond(404, {"status": "error", "message": "not found"})


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = "/mnt/usb/t7-1/media/books/metadata.db"


def create_server(port: int = 8787, db_path: str = DEFAULT_DB_PATH,
                   annotation_store=None) -> HTTPServer:
    """Create and return a configured HTTPServer (not yet started).

    Args:
        port:             TCP port to bind.
        db_path:          Path to Calibre's metadata.db (stored for later use).
        annotation_store: AnnotationStore instance for the reading services API.
                          If None, a default in-process store is created.

    Returns:
        An HTTPServer instance ready for serve_forever() or handle_request().
    """
    server = HTTPServer(("", port), _SyncHandler)
    server.db_path = db_path  # stash for handler access via self.server.db_path
    if annotation_store is None:
        annotation_store = AnnotationStore()
    server.annotation_store = annotation_store
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
    parser.add_argument("--annotation-db",
        default="/home/striblet/src/kobo-highlights-sync/kobo_annotations.db",
        help="Path to Kobo annotation store DB")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = AnnotationStore(args.annotation_db)
    server = create_server(port=args.port, db_path=args.db, annotation_store=store)
    logger.info("Listening on port %d (db=%s)", args.port, args.db)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
