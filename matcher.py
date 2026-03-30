"""matcher.py — Match Kobo book metadata to Calibre book IDs.

Three-tier strategy:
  1. Exact ISBN match via the identifiers table
  2. Exact normalized title + author match
  3. Fuzzy title (substring in either direction) + author match

All public functions return a Calibre book ID (int) or None.
"""

import logging
import re
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    text = text.lower()
    # Remove all characters that are not alphanumeric or whitespace
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse internal whitespace and strip leading/trailing
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_isbn(isbn: str) -> str:
    """Strip everything except digits and uppercase X (ISBN-10 check digit)."""
    return re.sub(r"[^0-9Xx]", "", isbn).upper()


def _author_matches(calibre_author_sort: str, kobo_author: Optional[str]) -> bool:
    """Return True if the Kobo author matches the Calibre author_sort field.

    Calibre stores authors as "Last, First"; Kobo sends "First Last".
    We compare by normalised word sets so order doesn't matter.
    If kobo_author is None, the caller didn't supply an author — skip the
    check and return True (don't filter on author).
    """
    if kobo_author is None:
        return True

    calibre_words = set(_normalize(calibre_author_sort).split())
    kobo_words = set(_normalize(kobo_author).split())

    # Remove the comma artifact that _normalize turns into a space — already
    # handled because punctuation is stripped, but be explicit: both sides
    # should be plain word sets at this point.
    return calibre_words == kobo_words


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_book(
    conn: sqlite3.Connection,
    isbn: Optional[str] = None,
    title: Optional[str] = None,
    author: Optional[str] = None,
) -> Optional[int]:
    """Match a Kobo book to a Calibre book ID.

    Parameters
    ----------
    conn:   Open sqlite3 connection to the Calibre metadata.db.
    isbn:   ISBN from Kobo (may contain hyphens or spaces).
    title:  Book title from Kobo.
    author: Author name from Kobo ("First Last" format).

    Returns
    -------
    Calibre book ID (int) or None if no match found.
    """
    # ------------------------------------------------------------------
    # Strategy 1 — ISBN match
    # ------------------------------------------------------------------
    if isbn:
        clean = _clean_isbn(isbn)
        if clean:
            row = conn.execute(
                "SELECT book FROM identifiers WHERE type = 'isbn' AND val = ?",
                (clean,),
            ).fetchone()
            if row:
                logger.debug("ISBN match: isbn=%s → book_id=%d", isbn, row[0])
                return row[0]
            logger.debug("No ISBN match for %s (cleaned: %s)", isbn, clean)

    # ------------------------------------------------------------------
    # Strategy 2 — Exact normalized title + author
    # ------------------------------------------------------------------
    if title:
        norm_title = _normalize(title)
        rows = conn.execute("SELECT id, title, author_sort FROM books").fetchall()

        for book_id, db_title, db_author_sort in rows:
            if _normalize(db_title) == norm_title and _author_matches(db_author_sort, author):
                logger.debug(
                    "Exact title match: title=%r author=%r → book_id=%d",
                    title, author, book_id,
                )
                return book_id

        # ------------------------------------------------------------------
        # Strategy 3 — Fuzzy title (substring in either direction) + author
        # ------------------------------------------------------------------
        for book_id, db_title, db_author_sort in rows:
            norm_db_title = _normalize(db_title)
            # Either the Kobo title is a substring of the Calibre title
            # or the Calibre title is a substring of the Kobo title.
            kobo_in_calibre = norm_title in norm_db_title
            calibre_in_kobo = norm_db_title in norm_title

            if (kobo_in_calibre or calibre_in_kobo) and _author_matches(db_author_sort, author):
                logger.debug(
                    "Fuzzy title match: title=%r author=%r → book_id=%d",
                    title, author, book_id,
                )
                return book_id

        logger.warning("No match found: title=%r author=%r isbn=%r", title, author, isbn)

    return None
