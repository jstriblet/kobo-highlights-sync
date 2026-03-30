"""Converts Kobo highlights to Calibre's annot_data format and writes to the annotations table."""

import json
import re
from datetime import datetime, timezone


def _parse_kobo_path(path: str):
    """Parse a Kobo path like 'OEBPS/ch.xhtml#point(/1/4/2:223)' into (spine_name, point).

    Returns (spine_name, point) where point is None if no fragment is present.
    """
    if "#" not in path:
        return path, None

    spine_name, fragment = path.split("#", 1)

    # Extract the point value from point(...) wrapper
    match = re.search(r"point\(([^)]+)\)", fragment)
    if match:
        point = match.group(1)
    else:
        # Fragment present but not a point() — return raw fragment
        point = fragment

    return spine_name, point


def _iso_to_epoch(iso_str: str) -> float:
    """Convert an ISO 8601 string (with Z or +00:00 offset) to a float epoch timestamp."""
    # Normalise 'Z' suffix to '+00:00' for fromisoformat (Python 3.7+ compat)
    normalised = iso_str
    if normalised.endswith("Z"):
        normalised = normalised[:-1] + "+00:00"

    dt = datetime.fromisoformat(normalised)

    # If no tzinfo, treat as UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.timestamp()


def kobo_to_calibre_annotation(kobo_highlight: dict) -> dict:
    """Convert one Kobo highlight dict to Calibre annotation fields.

    Returns a dict with keys: annot_id, annot_type, annot_data (JSON string),
    searchable_text, timestamp (epoch float).
    """
    bookmark_id = kobo_highlight["bookmark_id"]
    text = kobo_highlight["text"]
    annotation = kobo_highlight.get("annotation")
    date_created = kobo_highlight["date_created"]
    start_path = kobo_highlight.get("start_path", "")
    end_path = kobo_highlight.get("end_path", "")

    start_spine, start_cfi = _parse_kobo_path(start_path)
    _end_spine, end_cfi = _parse_kobo_path(end_path)

    # Build annot_data payload
    annot_data = {
        "uuid": bookmark_id,
        "type": "highlight",
        "highlighted_text": text,
        "timestamp": date_created,
        "style": {"kind": "color", "type": "builtin", "which": "yellow"},
        "spine_name": start_spine,
        "start_cfi": start_cfi,
        "end_cfi": end_cfi,
    }

    if annotation:
        annot_data["notes"] = annotation

    # searchable_text is the highlighted passage, plus the note if present
    if annotation:
        searchable_text = f"{text}\n{annotation}"
    else:
        searchable_text = text

    return {
        "annot_id": bookmark_id,
        "annot_type": "highlight",
        "annot_data": json.dumps(annot_data, ensure_ascii=False),
        "searchable_text": searchable_text,
        "timestamp": _iso_to_epoch(date_created),
    }


def write_annotations(conn, book_id: int, annotations: list) -> dict:
    """Write Calibre-format annotation dicts to the annotations table, skipping duplicates.

    Deduplicates on (annot_id, book).

    Returns {"inserted": N, "skipped": N}.
    """
    if not annotations:
        return {"inserted": 0, "skipped": 0}

    # Fetch all existing annot_ids for this book in one query
    existing = set(
        row[0]
        for row in conn.execute(
            "SELECT annot_id FROM annotations WHERE book = ?", (book_id,)
        )
    )

    inserted = 0
    skipped = 0

    for ann in annotations:
        if ann["annot_id"] in existing:
            skipped += 1
            continue

        conn.execute(
            """
            INSERT INTO annotations
                (book, format, user_type, user, timestamp, annot_id, annot_type, annot_data, searchable_text)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                book_id,
                "EPUB",
                "local",
                "kobo",
                ann["timestamp"],
                ann["annot_id"],
                ann["annot_type"],
                ann["annot_data"],
                ann["searchable_text"],
            ),
        )
        existing.add(ann["annot_id"])
        inserted += 1

    conn.commit()
    return {"inserted": inserted, "skipped": skipped}
