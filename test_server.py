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


if __name__ == "__main__":
    unittest.main()
