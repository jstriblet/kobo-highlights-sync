"""Tests for matcher.py — book matching (ISBN → title+author → fuzzy)."""

import sqlite3
import unittest

from matcher import match_book, _normalize, _author_matches


def _make_db():
    """Create an in-memory Calibre-like database with test fixtures."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE books (
            id          INTEGER PRIMARY KEY,
            title       TEXT NOT NULL,
            author_sort TEXT NOT NULL DEFAULT '',
            isbn        TEXT DEFAULT ''
        );
        CREATE TABLE identifiers (
            id   INTEGER PRIMARY KEY,
            book INTEGER NOT NULL,
            type TEXT NOT NULL DEFAULT 'isbn',
            val  TEXT NOT NULL
        );
        INSERT INTO books (id, title, author_sort) VALUES
            (1, 'Wind and Truth: Book Five of the Stormlight Archive', 'Sanderson, Brandon'),
            (2, 'The Quantum Thief', 'Rajaniemi, Hannu'),
            (3, 'Dune', 'Herbert, Frank');
        INSERT INTO identifiers (book, type, val) VALUES
            (1, 'isbn', '9781250319180'),
            (2, 'isbn', '9780575088894'),
            (3, 'isbn', '9780441013593');
    """)
    return conn


class TestNormalize(unittest.TestCase):
    def test_lowercase(self):
        self.assertEqual(_normalize("DUNE"), "dune")

    def test_strip_punctuation(self):
        self.assertEqual(_normalize("Hello, World!"), "hello world")

    def test_collapse_whitespace(self):
        self.assertEqual(_normalize("  too   many   spaces  "), "too many spaces")

    def test_colon_stripped(self):
        # Colons and subtitles are stripped
        result = _normalize("Wind and Truth: Book Five")
        self.assertNotIn(":", result)

    def test_empty_string(self):
        self.assertEqual(_normalize(""), "")


class TestAuthorMatches(unittest.TestCase):
    def test_last_first_vs_first_last(self):
        # Calibre stores "Sanderson, Brandon"; Kobo sends "Brandon Sanderson"
        self.assertTrue(_author_matches("Sanderson, Brandon", "Brandon Sanderson"))

    def test_reversed_name(self):
        self.assertTrue(_author_matches("Herbert, Frank", "Frank Herbert"))

    def test_mismatch(self):
        self.assertFalse(_author_matches("Herbert, Frank", "Brandon Sanderson"))

    def test_none_author(self):
        # If no author provided, skip the author check (treat as match)
        self.assertTrue(_author_matches("Herbert, Frank", None))

    def test_case_insensitive(self):
        self.assertTrue(_author_matches("Herbert, Frank", "frank herbert"))


class TestMatchBook(unittest.TestCase):
    def setUp(self):
        self.conn = _make_db()

    def tearDown(self):
        self.conn.close()

    # --- Strategy 1: ISBN ---

    def test_isbn_exact(self):
        result = match_book(self.conn, isbn="9781250319180")
        self.assertEqual(result, 1)

    def test_isbn_book2(self):
        result = match_book(self.conn, isbn="9780575088894")
        self.assertEqual(result, 2)

    def test_isbn_book3(self):
        result = match_book(self.conn, isbn="9780441013593")
        self.assertEqual(result, 3)

    def test_isbn_strips_hyphens(self):
        # ISBNs with hyphens should still match
        result = match_book(self.conn, isbn="978-1-250-31918-0")
        self.assertEqual(result, 1)

    def test_isbn_not_found(self):
        result = match_book(self.conn, isbn="0000000000000")
        self.assertIsNone(result)

    # --- Strategy 2: Title + Author (exact normalized) ---

    def test_title_author_exact(self):
        result = match_book(self.conn, title="The Quantum Thief", author="Hannu Rajaniemi")
        self.assertEqual(result, 2)

    def test_title_author_case_insensitive(self):
        result = match_book(self.conn, title="the quantum thief", author="hannu rajaniemi")
        self.assertEqual(result, 2)

    def test_title_only_exact(self):
        result = match_book(self.conn, title="Dune")
        self.assertEqual(result, 3)

    def test_title_author_dune(self):
        result = match_book(self.conn, title="dune", author="frank herbert")
        self.assertEqual(result, 3)

    # --- Strategy 3: Fuzzy (substring in either direction) ---

    def test_fuzzy_kobo_title_is_substring_of_calibre(self):
        # Kobo sends "Wind and Truth"; Calibre has full subtitle
        result = match_book(self.conn, title="Wind and Truth", author="Brandon Sanderson")
        self.assertEqual(result, 1)

    def test_fuzzy_calibre_title_is_substring_of_kobo(self):
        # Calibre has "Dune"; Kobo might send "Dune: The Original Classic"
        result = match_book(self.conn, title="Dune: The Original Classic", author="Frank Herbert")
        self.assertEqual(result, 3)

    # --- No match ---

    def test_no_match(self):
        result = match_book(self.conn, title="Nonexistent Book", author="Nobody")
        self.assertIsNone(result)

    def test_all_none(self):
        result = match_book(self.conn)
        self.assertIsNone(result)

    def test_isbn_none_title_none(self):
        result = match_book(self.conn, isbn=None, title=None, author=None)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
