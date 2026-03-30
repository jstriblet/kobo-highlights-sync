"""Tests for the Kobo highlights sync server."""

import json
import threading
import unittest
import urllib.request
import urllib.error

from annotation_store import AnnotationStore
from server import create_server


def _start_server(port=18787, db_path=":memory:", annotation_store=None):
    """Start a test server on the given port and return (server, thread)."""
    if annotation_store is None:
        annotation_store = AnnotationStore(":memory:")
    server = create_server(port=port, db_path=db_path, annotation_store=annotation_store)
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

    def _post(self, path_or_body, body=None):
        if body is None:
            # Called as _post(body) — legacy signature
            path = "/sync"
            body = path_or_body
        else:
            # Called as _post(path, body)
            path = path_or_body
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{self.base}{path}",
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

    def _get(self, path):
        req = urllib.request.Request(
            f"{self.base}{path}",
            method="GET",
        )
        try:
            resp = urllib.request.urlopen(req)
            return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_get_highlights_for_book(self):
        # First sync a highlight
        self._post("/sync", {
            "device_id": "kobo",
            "highlights": [{
                "bookmark_id": "get-test-1",
                "text": "Test passage for GET",
                "annotation": None,
                "date_created": "2026-03-28T12:00:00Z",
                "chapter_progress": 0.3,
                "content_id": "",
                "start_path": "",
                "end_path": "",
                "book_title": "Dune",
                "book_author": "Frank Herbert",
                "book_isbn": "9780441013593",
            }]
        })
        # Then fetch
        status, body = self._get("/highlights?book_id=1")
        self.assertEqual(status, 200)
        self.assertGreater(len(body["highlights"]), 0)
        self.assertEqual(body["highlights"][0]["highlighted_text"], "Test passage for GET")

    def test_get_highlights_all(self):
        """GET /highlights without book_id returns up to 100 highlights."""
        status, body = self._get("/highlights")
        self.assertEqual(status, 200)
        self.assertIn("highlights", body)
        self.assertIn("count", body)

    def test_get_highlights_unknown_book(self):
        """GET /highlights?book_id=9999 returns empty list for unknown book."""
        status, body = self._get("/highlights?book_id=9999")
        self.assertEqual(status, 200)
        self.assertEqual(body["highlights"], [])
        self.assertEqual(body["count"], 0)


class TestReadingServices(unittest.TestCase):
    """Reading services API: checkforchanges, GET/PATCH annotations."""

    _SETUP_SQL = """
        CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            title TEXT,
            author_sort TEXT DEFAULT '',
            uuid TEXT
        );
        CREATE TABLE identifiers (
            id INTEGER PRIMARY KEY,
            book INTEGER,
            type TEXT DEFAULT 'isbn',
            val TEXT
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
            searchable_text TEXT DEFAULT ''
        );
        INSERT INTO books (id, title, author_sort, uuid)
        VALUES (129, 'Empire of Silence', 'Ruocchio, Christopher',
                'dc31e00d-e279-405d-befb-346da52b10a0');
    """

    _CONTENT_ID = "dc31e00d-e279-405d-befb-346da52b10a0"

    _SAMPLE_ANNOTATION = {
        "clientLastModifiedUtc": "2026-03-30T03:24:18Z",
        "highlightColor": "#B2E1E8",
        "highlightedText": "NARROW WINDOWS OF Gibson's cloister cell stood open, looking",
        "id": "29621ba2-009e-4c85-a7f1-5a084635f889",
        "location": {
            "span": {
                "chapterFilename": "OEBPS/xhtml/09_Chapter_2_Like_Distan.xhtml",
                "chapterProgress": 0.1,
                "chapterTitle": "Chapter 2: Like Distant Thunder",
                "endChar": 42,
                "endPath": "span#kobo.3.2",
                "startChar": 3,
                "startPath": "span#kobo.3.1",
            }
        },
        "type": "highlight",
    }

    @classmethod
    def setUpClass(cls):
        import sqlite3
        import tempfile
        import os

        fd, cls._db_file = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        conn = sqlite3.connect(cls._db_file)
        conn.executescript(cls._SETUP_SQL)
        conn.close()

        cls._store = AnnotationStore(":memory:")
        cls.server, cls.thread = _start_server(
            port=18925, db_path=cls._db_file, annotation_store=cls._store
        )
        cls.base = "http://127.0.0.1:18925"

    @classmethod
    def tearDownClass(cls):
        import os
        cls.server.shutdown()
        os.unlink(cls._db_file)

    def _request(self, method, path, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            headers={"Content-Type": "application/json", **(headers or {})},
            method=method,
        )
        try:
            resp = urllib.request.urlopen(req)
            return resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    # -- checkforchanges ---------------------------------------------------

    def test_checkforchanges_returns_changed(self):
        """POST checkforchanges returns flat array of changed ContentIds."""
        # Seed one annotation so the store has a non-empty etag for our book
        self._store.upsert(self._CONTENT_ID, [self._SAMPLE_ANNOTATION])

        payload = [
            {"ContentId": self._CONTENT_ID, "etag": "wrong-etag"},
            {"ContentId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "etag": 'W/"0"'},
        ]
        status, headers, raw = self._request("POST", "/api/v3/content/checkforchanges", payload)
        self.assertEqual(status, 200)
        changed = json.loads(raw)
        # First book has wrong etag -> changed; second has matching empty etag -> not changed
        self.assertIn(self._CONTENT_ID, changed)
        self.assertNotIn("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", changed)

    # -- PATCH annotations -------------------------------------------------

    def test_patch_annotations_stores_highlight(self):
        """PATCH stores a highlight and returns 200 with etag."""
        body = {"updatedAnnotations": [self._SAMPLE_ANNOTATION]}
        status, headers, raw = self._request(
            "PATCH",
            f"/api/v3/content/{self._CONTENT_ID}/annotations",
            body,
        )
        self.assertEqual(status, 200)
        self.assertTrue(headers.get("etag"), "Response must include etag header")

    # -- GET annotations ---------------------------------------------------

    def test_get_annotations_returns_stored(self):
        """GET after PATCH returns the stored highlight."""
        # Ensure there's data (may already exist from prior tests)
        self._store.upsert(self._CONTENT_ID, [self._SAMPLE_ANNOTATION])

        status, headers, raw = self._request(
            "GET",
            f"/api/v3/content/{self._CONTENT_ID}/annotations",
        )
        self.assertEqual(status, 200)
        body = json.loads(raw)
        self.assertIn("annotations", body)
        texts = [a.get("highlightedText", "") for a in body["annotations"]]
        self.assertIn(self._SAMPLE_ANNOTATION["highlightedText"], texts)

    def test_get_annotations_304_when_unchanged(self):
        """GET with matching If-None-Match returns 304."""
        etag = self._store.get_etag(self._CONTENT_ID)
        status, headers, raw = self._request(
            "GET",
            f"/api/v3/content/{self._CONTENT_ID}/annotations",
            headers={"If-None-Match": etag},
        )
        self.assertEqual(status, 304)

    # -- Calibre write-through ---------------------------------------------

    def test_patch_writes_to_calibre(self):
        """PATCH writes the highlight to Calibre's annotations table."""
        import sqlite3

        # Use a unique annotation id to avoid collisions with other tests
        ann = dict(self._SAMPLE_ANNOTATION)
        ann["id"] = "calibre-write-test-0001"
        ann["highlightedText"] = "Calibre write-through test"

        body = {"updatedAnnotations": [ann]}
        status, _, _ = self._request(
            "PATCH",
            f"/api/v3/content/{self._CONTENT_ID}/annotations",
            body,
        )
        self.assertEqual(status, 200)

        # Verify in Calibre metadata.db
        conn = sqlite3.connect(self._db_file)
        try:
            row = conn.execute(
                "SELECT annot_data FROM annotations WHERE book=129 AND annot_id=?",
                ("calibre-write-test-0001",),
            ).fetchone()
            self.assertIsNotNone(row, "Annotation should exist in Calibre DB")
            data = json.loads(row[0])
            self.assertEqual(data["highlighted_text"], "Calibre write-through test")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
