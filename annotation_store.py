"""Kobo annotation store backed by SQLite.

Stores Kobo-format annotations received via the reading services API.  Used by
the sync server to:
  - Persist annotations from Kobo PATCH requests
  - Serve them back on GET so the device doesn't wipe its local copy
  - Compute ETags for change detection

Default database path: kobo_annotations.db (separate from Calibre's metadata.db)
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone


DEFAULT_DB_PATH = "/home/striblet/src/kobo-highlights-sync/kobo_annotations.db"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS annotations (
    id          TEXT NOT NULL,
    content_id  TEXT NOT NULL,
    type        TEXT NOT NULL,
    data        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (id, content_id)
);
"""


class AnnotationStore:
    """Persistent store for Kobo annotations."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """Open (or create) the SQLite database and ensure the schema exists.

        Args:
            db_path: Path to the SQLite file, or ':memory:' for tests.
        """
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_CREATE_TABLE_SQL)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert(self, content_id: str, annotations: list) -> dict:
        """Store annotations from a Kobo PATCH request.

        Inserts new annotations and updates existing ones (matched by id +
        content_id).  The caller supplies the raw list from the Kobo's
        ``updatedAnnotations`` field; each item is stored verbatim as JSON.

        Args:
            content_id:  Kobo book ContentId (UUID string).
            annotations: List of annotation dicts from the Kobo request.
                         Each must have at least: id, type.

        Returns:
            dict with keys ``inserted`` and ``updated`` (int counts).
        """
        inserted = 0
        updated = 0
        now = datetime.now(timezone.utc).isoformat()

        for ann in annotations:
            ann_id = ann["id"]
            ann_type = ann.get("type", "highlight")
            data_json = json.dumps(ann)

            existing = self._conn.execute(
                "SELECT 1 FROM annotations WHERE id = ? AND content_id = ?",
                (ann_id, content_id),
            ).fetchone()

            if existing is None:
                self._conn.execute(
                    "INSERT INTO annotations (id, content_id, type, data, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (ann_id, content_id, ann_type, data_json, now),
                )
                inserted += 1
            else:
                self._conn.execute(
                    "UPDATE annotations SET type = ?, data = ? "
                    "WHERE id = ? AND content_id = ?",
                    (ann_type, data_json, ann_id, content_id),
                )
                updated += 1

        self._conn.commit()
        return {"inserted": inserted, "updated": updated}

    def get_annotations(self, content_id: str) -> list:
        """Return all annotations for a book in Kobo API format.

        Args:
            content_id: Kobo book ContentId (UUID string).

        Returns:
            List of annotation dicts parsed from the stored JSON.
            Empty list if no annotations exist for this content_id.
        """
        rows = self._conn.execute(
            "SELECT data FROM annotations WHERE content_id = ? ORDER BY created_at",
            (content_id,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def get_etag(self, content_id: str) -> str:
        """Compute a weak ETag for a book's current annotations.

        The ETag is a hash over sorted (id, data) pairs so it changes whenever
        an annotation is added, removed, or modified.

        Args:
            content_id: Kobo book ContentId (UUID string).

        Returns:
            A weak ETag string like ``'W/"<hex>"'``, or ``'W/"0"'`` when
            there are no annotations.
        """
        rows = self._conn.execute(
            "SELECT id, data FROM annotations WHERE content_id = ? ORDER BY id",
            (content_id,),
        ).fetchall()

        if not rows:
            return 'W/"0"'

        digest = hashlib.sha1()
        for ann_id, data in rows:
            digest.update(ann_id.encode())
            digest.update(data.encode())

        return f'W/"{digest.hexdigest()}"'

    def get_all_content_ids(self) -> set:
        """Return the set of all content_ids that have at least one annotation."""
        rows = self._conn.execute(
            "SELECT DISTINCT content_id FROM annotations"
        ).fetchall()
        return {row[0] for row in rows}
