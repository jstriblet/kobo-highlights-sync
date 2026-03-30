"""Tests for the Kobo → Calibre annotation writer."""

import json
import sqlite3
import unittest

from writer import kobo_to_calibre_annotation, write_annotations, _parse_kobo_path, _iso_to_epoch


SAMPLE_HIGHLIGHT = {
    "bookmark_id": "abc123-def456",
    "text": "It was the best of times, it was the worst of times.",
    "annotation": None,
    "date_created": "2024-03-15T10:30:00Z",
    "chapter_progress": 0.12,
    "content_id": "file:///mnt/onboard/Books/tale.epub!OEBPS/chapter01.xhtml",
    "start_path": "OEBPS/chapter01.xhtml#point(/1/4/2/26/1:223)",
    "end_path": "OEBPS/chapter01.xhtml#point(/1/4/2/28/1:45)",
}

SAMPLE_HIGHLIGHT_WITH_NOTE = {
    "bookmark_id": "bbb999-fff000",
    "text": "All happy families are alike.",
    "annotation": "Great opening line",
    "date_created": "2024-04-01T08:00:00+00:00",
    "chapter_progress": 0.02,
    "content_id": "file:///mnt/onboard/Books/anna.epub!OEBPS/part1.xhtml",
    "start_path": "OEBPS/part1.xhtml#point(/1/2/1:0)",
    "end_path": "OEBPS/part1.xhtml#point(/1/2/1:29)",
}


def _make_db():
    """Create an in-memory Calibre-like annotations table."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
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
        )
    """)
    conn.commit()
    return conn


class TestParseKoboPath(unittest.TestCase):
    """_parse_kobo_path extracts spine_name and point."""

    def test_basic_path(self):
        spine, point = _parse_kobo_path("OEBPS/chapter01.xhtml#point(/1/4/2/26/1:223)")
        self.assertEqual(spine, "OEBPS/chapter01.xhtml")
        self.assertEqual(point, "/1/4/2/26/1:223")

    def test_path_without_fragment(self):
        spine, point = _parse_kobo_path("OEBPS/chapter01.xhtml")
        self.assertEqual(spine, "OEBPS/chapter01.xhtml")
        self.assertIsNone(point)

    def test_short_point(self):
        spine, point = _parse_kobo_path("OEBPS/part1.xhtml#point(/1/2/1:0)")
        self.assertEqual(spine, "OEBPS/part1.xhtml")
        self.assertEqual(point, "/1/2/1:0")


class TestIsoToEpoch(unittest.TestCase):
    """_iso_to_epoch converts ISO 8601 strings to float timestamps."""

    def test_z_suffix(self):
        epoch = _iso_to_epoch("2024-03-15T10:30:00Z")
        self.assertIsInstance(epoch, float)
        self.assertGreater(epoch, 0)

    def test_plus_offset(self):
        epoch = _iso_to_epoch("2024-04-01T08:00:00+00:00")
        self.assertIsInstance(epoch, float)
        self.assertGreater(epoch, 0)

    def test_z_and_offset_equal(self):
        epoch_z = _iso_to_epoch("2024-03-15T10:30:00Z")
        epoch_offset = _iso_to_epoch("2024-03-15T10:30:00+00:00")
        self.assertAlmostEqual(epoch_z, epoch_offset, places=1)


class TestKoboToCalibreAnnotation(unittest.TestCase):
    """kobo_to_calibre_annotation converts a Kobo highlight dict."""

    def test_highlight_only_annot_type(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        self.assertEqual(result["annot_type"], "highlight")

    def test_highlight_only_annot_id(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        self.assertEqual(result["annot_id"], "abc123-def456")

    def test_highlight_only_annot_data_is_valid_json(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertIsInstance(data, dict)

    def test_highlight_only_annot_data_uuid(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["uuid"], "abc123-def456")

    def test_highlight_only_annot_data_type(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["type"], "highlight")

    def test_highlight_only_annot_data_highlighted_text(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["highlighted_text"], SAMPLE_HIGHLIGHT["text"])

    def test_highlight_only_annot_data_timestamp(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["timestamp"], "2024-03-15T10:30:00Z")

    def test_highlight_only_annot_data_style(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["style"], {"kind": "color", "type": "builtin", "which": "yellow"})

    def test_highlight_only_annot_data_spine_name(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["spine_name"], "OEBPS/chapter01.xhtml")

    def test_highlight_only_annot_data_start_cfi(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["start_cfi"], "/1/4/2/26/1:223")

    def test_highlight_only_annot_data_end_cfi(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertEqual(data["end_cfi"], "/1/4/2/28/1:45")

    def test_highlight_only_no_notes_field(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        data = json.loads(result["annot_data"])
        self.assertNotIn("notes", data)

    def test_highlight_only_searchable_text(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        self.assertEqual(result["searchable_text"], SAMPLE_HIGHLIGHT["text"])

    def test_highlight_only_timestamp_is_float(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT)
        self.assertIsInstance(result["timestamp"], float)

    def test_with_note_annot_type(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT_WITH_NOTE)
        self.assertEqual(result["annot_type"], "highlight")

    def test_with_note_includes_notes_field(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT_WITH_NOTE)
        data = json.loads(result["annot_data"])
        self.assertIn("notes", data)
        self.assertEqual(data["notes"], "Great opening line")

    def test_with_note_searchable_text_includes_note(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT_WITH_NOTE)
        self.assertIn("Great opening line", result["searchable_text"])

    def test_with_note_searchable_text_includes_highlight(self):
        result = kobo_to_calibre_annotation(SAMPLE_HIGHLIGHT_WITH_NOTE)
        self.assertIn("All happy families are alike.", result["searchable_text"])


class TestWriteAnnotations(unittest.TestCase):
    """write_annotations inserts rows and deduplicates."""

    def _sample_annotations(self, n=1, start_id=0):
        results = []
        for i in range(start_id, start_id + n):
            hl = {
                "bookmark_id": f"id-{i:04d}",
                "text": f"Highlight text number {i}",
                "annotation": None,
                "date_created": "2024-01-01T00:00:00Z",
                "chapter_progress": 0.1 * i,
                "content_id": f"file:///book.epub!OEBPS/ch{i}.xhtml",
                "start_path": f"OEBPS/ch{i}.xhtml#point(/1/2/1:0)",
                "end_path": f"OEBPS/ch{i}.xhtml#point(/1/2/1:10)",
            }
            results.append(kobo_to_calibre_annotation(hl))
        return results

    def test_insert_single_new(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        result = write_annotations(conn, book_id=42, annotations=annotations)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["skipped"], 0)

    def test_insert_single_new_row_in_db(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT COUNT(*) FROM annotations").fetchone()[0]
        self.assertEqual(row, 1)

    def test_deduplication_same_annot_id(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        # Insert same annotation again
        result = write_annotations(conn, book_id=42, annotations=annotations)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["skipped"], 1)

    def test_deduplication_only_one_row(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT COUNT(*) FROM annotations").fetchone()[0]
        self.assertEqual(row, 1)

    def test_insert_five(self):
        conn = _make_db()
        annotations = self._sample_annotations(5)
        result = write_annotations(conn, book_id=42, annotations=annotations)
        self.assertEqual(result["inserted"], 5)
        self.assertEqual(result["skipped"], 0)

    def test_insert_five_rows_in_db(self):
        conn = _make_db()
        annotations = self._sample_annotations(5)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT COUNT(*) FROM annotations").fetchone()[0]
        self.assertEqual(row, 5)

    def test_format_is_epub(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT format FROM annotations").fetchone()[0]
        self.assertEqual(row, "EPUB")

    def test_user_type_is_local(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT user_type FROM annotations").fetchone()[0]
        self.assertEqual(row, "local")

    def test_user_is_kobo(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=42, annotations=annotations)
        row = conn.execute("SELECT user FROM annotations").fetchone()[0]
        self.assertEqual(row, "kobo")

    def test_book_id_stored(self):
        conn = _make_db()
        annotations = self._sample_annotations(1)
        write_annotations(conn, book_id=99, annotations=annotations)
        row = conn.execute("SELECT book FROM annotations").fetchone()[0]
        self.assertEqual(row, 99)

    def test_empty_list(self):
        conn = _make_db()
        result = write_annotations(conn, book_id=42, annotations=[])
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["skipped"], 0)

    def test_mixed_new_and_duplicate(self):
        conn = _make_db()
        first_batch = self._sample_annotations(3)
        write_annotations(conn, book_id=42, annotations=first_batch)
        # Overlap: 2 existing + 2 new
        second_batch = self._sample_annotations(2) + self._sample_annotations(2, start_id=10)
        result = write_annotations(conn, book_id=42, annotations=second_batch)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["skipped"], 2)


if __name__ == "__main__":
    unittest.main()
