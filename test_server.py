"""Tests for the Kobo highlights sync server."""

import json
import threading
import unittest
import urllib.request
import urllib.error

from server import create_server


def _start_server(port=18787):
    """Start a test server on the given port and return (server, thread)."""
    server = create_server(port=port, db_path=":memory:")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


class TestHealth(unittest.TestCase):
    """GET /health endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.thread = _start_server(port=18787)
        cls.base = "http://127.0.0.1:18787"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_health_200(self):
        resp = urllib.request.urlopen(f"{self.base}/health")
        self.assertEqual(resp.status, 200)

    def test_health_body(self):
        resp = urllib.request.urlopen(f"{self.base}/health")
        body = json.loads(resp.read())
        self.assertEqual(body, {"status": "ok"})

    def test_health_content_type(self):
        resp = urllib.request.urlopen(f"{self.base}/health")
        self.assertIn("application/json", resp.headers.get("Content-Type", ""))


class TestSyncEndpoint(unittest.TestCase):
    """POST /sync endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.thread = _start_server(port=18788)
        cls.base = "http://127.0.0.1:18788"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _post(self, body, content_type="application/json"):
        data = body.encode() if isinstance(body, str) else body
        req = urllib.request.Request(
            f"{self.base}/sync",
            data=data,
            headers={"Content-Type": content_type},
            method="POST",
        )
        try:
            resp = urllib.request.urlopen(req)
            return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_sync_empty_highlights_200(self):
        payload = json.dumps({"device_id": "kobo-clara-color", "highlights": []})
        status, body = self._post(payload)
        self.assertEqual(status, 200)

    def test_sync_empty_highlights_body(self):
        payload = json.dumps({"device_id": "kobo-clara-color", "highlights": []})
        status, body = self._post(payload)
        self.assertEqual(body["status"], "ok")

    def test_sync_received_count_zero(self):
        payload = json.dumps({"device_id": "kobo-clara-color", "highlights": []})
        status, body = self._post(payload)
        self.assertEqual(body["received"], 0)

    def test_sync_received_count_nonzero(self):
        highlights = [
            {"text": "Hello world", "book": "Test Book"},
            {"text": "Another line", "book": "Test Book"},
        ]
        payload = json.dumps({"device_id": "kobo-clara-color", "highlights": highlights})
        status, body = self._post(payload)
        self.assertEqual(status, 200)
        self.assertEqual(body["received"], 2)

    def test_sync_empty_body_400(self):
        status, body = self._post(b"")
        self.assertEqual(status, 400)

    def test_sync_invalid_json_400(self):
        status, body = self._post("not json at all")
        self.assertEqual(status, 400)

    def test_sync_missing_device_id_400(self):
        payload = json.dumps({"highlights": []})
        status, body = self._post(payload)
        self.assertEqual(status, 400)

    def test_sync_missing_highlights_400(self):
        payload = json.dumps({"device_id": "kobo-clara-color"})
        status, body = self._post(payload)
        self.assertEqual(status, 400)


class TestUnknownPaths(unittest.TestCase):
    """404 for unrecognised paths."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.thread = _start_server(port=18789)
        cls.base = "http://127.0.0.1:18789"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_unknown_get_404(self):
        try:
            urllib.request.urlopen(f"{self.base}/unknown")
            self.fail("Expected HTTPError")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)

    def test_wrong_method_on_health_405(self):
        req = urllib.request.Request(
            f"{self.base}/health",
            data=b"",
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            self.fail("Expected HTTPError")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 405)

    def test_wrong_method_on_sync_405(self):
        try:
            urllib.request.urlopen(f"{self.base}/sync")
            self.fail("Expected HTTPError")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 405)


class TestFullSync(unittest.TestCase):
    """Integration tests: POST /sync performs a full round-trip via matcher + writer."""

    # SQL to build a minimal Calibre metadata.db in memory
    _SETUP_SQL = """
        CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            author_sort TEXT NOT NULL DEFAULT '',
            isbn TEXT DEFAULT ''
        );
        CREATE TABLE identifiers (
            id INTEGER PRIMARY KEY,
            book INTEGER NOT NULL,
            type TEXT NOT NULL DEFAULT 'isbn',
            val TEXT NOT NULL
        );
        CREATE TABLE annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book INTEGER NOT NULL,
            format TEXT NOT NULL,
            user_type TEXT NOT NULL,
            user TEXT NOT NULL,
            timestamp REAL NOT NULL,
            annot_id TEXT NOT NULL,
            annot_type TEXT NOT NULL,
            annot_data TEXT NOT NULL,
            searchable_text TEXT NOT NULL DEFAULT ''
        );
        INSERT INTO books (id, title, author_sort) VALUES (1, 'Dune', 'Herbert, Frank');
        INSERT INTO identifiers (book, type, val) VALUES (1, 'isbn', '9780441013593');
    """

    @classmethod
    def setUpClass(cls):
        import sqlite3
        import tempfile
        import os

        # Use a real temp file so the server and test can share it
        fd, cls._db_file = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        conn = sqlite3.connect(cls._db_file)
        conn.executescript(cls._SETUP_SQL)
        conn.close()

        cls.server, cls.thread = _start_server(port=18924)
        # Replace the server's db_path with our temp file
        cls.server.db_path = cls._db_file
        cls.base = "http://127.0.0.1:18924"

    @classmethod
    def tearDownClass(cls):
        import os
        cls.server.shutdown()
        os.unlink(cls._db_file)

    def _post(self, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{self.base}/sync",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            resp = urllib.request.urlopen(req)
            return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def _make_highlight(self, **overrides):
        """Return a minimal Kobo highlight dict, with optional field overrides."""
        base = {
            "bookmark_id": "test-bm-001",
            "text": "I must not fear. Fear is the mind-killer.",
            "annotation": None,
            "date_created": "2024-06-01T12:00:00Z",
            "chapter_progress": 0.1,
            "content_id": "file:///mnt/sd/dune.epub!OEBPS/ch01.xhtml",
            "start_path": "OEBPS/ch01.xhtml#point(/1/4/2:0)",
            "end_path": "OEBPS/ch01.xhtml#point(/1/4/2:42)",
            "book_title": "Dune",
            "book_author": "Frank Herbert",
            "book_isbn": "9780441013593",
        }
        base.update(overrides)
        return base

    def test_sync_highlights_end_to_end(self):
        """Matched highlight is inserted; response contains correct results entry."""
        payload = {
            "device_id": "kobo-test",
            "highlights": [self._make_highlight()],
        }
        status, body = self._post(payload)
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["received"], 1)
        self.assertEqual(len(body["results"]), 1)
        result = body["results"][0]
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["book_title"], "Dune")
        self.assertEqual(result["book_id"], 1)
        self.assertEqual(len(body["unmatched"]), 0)

    def test_sync_unmatched_book(self):
        """Highlight for an unknown book ends up in the unmatched list."""
        payload = {
            "device_id": "kobo-test",
            "highlights": [
                self._make_highlight(
                    bookmark_id="test-bm-999",
                    book_title="Unknown Book XYZ",
                    book_author="Nobody Famous",
                    book_isbn="",
                )
            ],
        }
        status, body = self._post(payload)
        self.assertEqual(status, 200)
        self.assertEqual(body["received"], 1)
        self.assertEqual(len(body["results"]), 0)
        self.assertEqual(len(body["unmatched"]), 1)
        self.assertEqual(body["unmatched"][0]["title"], "Unknown Book XYZ")
        self.assertEqual(body["unmatched"][0]["count"], 1)


if __name__ == "__main__":
    unittest.main()
