"""Tests for annotation_store.py — Kobo annotation persistence layer."""

import unittest

from annotation_store import AnnotationStore


SAMPLE_ANNOTATION = {
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

CONTENT_ID = "a1b2c3d4-0000-0000-0000-000000000001"


class TestAnnotationStoreInit(unittest.TestCase):
    """Test 1: Create store with in-memory DB."""

    def test_create_in_memory(self):
        store = AnnotationStore(db_path=":memory:")
        self.assertIsNotNone(store)

    def test_empty_store_returns_no_content_ids(self):
        store = AnnotationStore(db_path=":memory:")
        self.assertEqual(store.get_all_content_ids(), set())


class TestUpsert(unittest.TestCase):
    """Tests 2–4, 8: Upsert behaviour."""

    def setUp(self):
        self.store = AnnotationStore(db_path=":memory:")

    # Test 2: single annotation → inserted=1, updated=0
    def test_upsert_single_inserted(self):
        result = self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["updated"], 0)

    # Test 3: same annotation again → inserted=0, updated=1
    def test_upsert_same_annotation_is_update(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        result = self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 1)

    # Test 8: multiple annotations → correct counts
    def test_upsert_multiple(self):
        second = dict(SAMPLE_ANNOTATION, id="aaaaaaaa-0000-0000-0000-000000000002")
        result = self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION, second])
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["updated"], 0)

    def test_upsert_mixed_insert_and_update(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        second = dict(SAMPLE_ANNOTATION, id="aaaaaaaa-0000-0000-0000-000000000002")
        result = self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION, second])
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["updated"], 1)

    def test_upsert_empty_list(self):
        result = self.store.upsert(CONTENT_ID, [])
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 0)


class TestGetAnnotations(unittest.TestCase):
    """Tests 4–5: get_annotations."""

    def setUp(self):
        self.store = AnnotationStore(db_path=":memory:")

    # Test 4: get annotations returns the stored data
    def test_get_returns_stored_annotation(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        annotations = self.store.get_annotations(CONTENT_ID)
        self.assertEqual(len(annotations), 1)
        ann = annotations[0]
        self.assertEqual(ann["id"], SAMPLE_ANNOTATION["id"])
        self.assertEqual(ann["type"], SAMPLE_ANNOTATION["type"])
        self.assertEqual(ann["highlightedText"], SAMPLE_ANNOTATION["highlightedText"])

    # Test 5: unknown content_id returns empty list
    def test_get_unknown_content_id_returns_empty(self):
        annotations = self.store.get_annotations("nonexistent-id")
        self.assertEqual(annotations, [])

    def test_get_returns_all_for_content_id(self):
        second = dict(SAMPLE_ANNOTATION, id="aaaaaaaa-0000-0000-0000-000000000002")
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION, second])
        annotations = self.store.get_annotations(CONTENT_ID)
        self.assertEqual(len(annotations), 2)

    def test_get_is_scoped_to_content_id(self):
        other_content_id = "ffffffff-0000-0000-0000-000000000099"
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        second = dict(SAMPLE_ANNOTATION, id="aaaaaaaa-0000-0000-0000-000000000002")
        self.store.upsert(other_content_id, [second])
        annotations = self.store.get_annotations(CONTENT_ID)
        self.assertEqual(len(annotations), 1)
        self.assertEqual(annotations[0]["id"], SAMPLE_ANNOTATION["id"])


class TestGetEtag(unittest.TestCase):
    """Tests 6–7: get_etag."""

    def setUp(self):
        self.store = AnnotationStore(db_path=":memory:")

    # Test 6: etag for no annotations is 'W/"0"'
    def test_etag_no_annotations(self):
        etag = self.store.get_etag(CONTENT_ID)
        self.assertEqual(etag, 'W/"0"')

    # Test 7: etag is non-zero string after upsert
    def test_etag_after_upsert(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        etag = self.store.get_etag(CONTENT_ID)
        self.assertNotEqual(etag, 'W/"0"')
        self.assertTrue(etag.startswith('W/"'))
        self.assertTrue(etag.endswith('"'))

    def test_etag_changes_when_annotation_updated(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        etag1 = self.store.get_etag(CONTENT_ID)
        modified = dict(SAMPLE_ANNOTATION, clientLastModifiedUtc="2026-04-01T00:00:00Z")
        self.store.upsert(CONTENT_ID, [modified])
        etag2 = self.store.get_etag(CONTENT_ID)
        self.assertNotEqual(etag1, etag2)

    def test_etag_stable_when_no_changes(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        etag1 = self.store.get_etag(CONTENT_ID)
        etag2 = self.store.get_etag(CONTENT_ID)
        self.assertEqual(etag1, etag2)


class TestGetAllContentIds(unittest.TestCase):
    """Test 9: get_all_content_ids."""

    def setUp(self):
        self.store = AnnotationStore(db_path=":memory:")

    def test_empty_store(self):
        self.assertEqual(self.store.get_all_content_ids(), set())

    def test_returns_content_id_after_upsert(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        ids = self.store.get_all_content_ids()
        self.assertIn(CONTENT_ID, ids)

    def test_returns_all_content_ids(self):
        other = "ffffffff-0000-0000-0000-000000000099"
        second = dict(SAMPLE_ANNOTATION, id="aaaaaaaa-0000-0000-0000-000000000002")
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        self.store.upsert(other, [second])
        ids = self.store.get_all_content_ids()
        self.assertEqual(ids, {CONTENT_ID, other})

    def test_duplicate_upserts_dont_duplicate_content_id(self):
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        self.store.upsert(CONTENT_ID, [SAMPLE_ANNOTATION])
        ids = self.store.get_all_content_ids()
        self.assertEqual(ids, {CONTENT_ID})


if __name__ == "__main__":
    unittest.main()
