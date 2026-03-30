# Kobo Highlights Sync for Calibre-Web

Automatically sync highlights and notes from your Kobo e-reader to [Calibre-Web](https://github.com/janeczku/calibre-web) — triggered by the built-in Sync button on the Kobo. No extra software on the Kobo required.

<img width="849" height="909" alt="Screenshot 2026-03-30 at 12 27 04 AM" src="https://github.com/user-attachments/assets/ae552aa7-69a9-45a4-b087-ba333adc8d4e" />

## How it works

When you tap Sync on your Kobo, it contacts a "reading services" API to sync annotations. Normally this goes to Kobo's cloud servers (`readingservices.kobo.com`). This project redirects that traffic to a local server that:

1. **Receives highlights** from the Kobo via the native annotation sync protocol
2. **Stores them locally** in a SQLite database (so the Kobo doesn't wipe its local copy on next sync)
3. **Writes them to Calibre's `metadata.db`** annotations table
4. **Displays them in Calibre-Web** on each book's detail page via a template patch

```
Kobo Clara Color                    Your Server
┌──────────────┐    WiFi Sync     ┌─────────────────────┐
│              │ ───────────────> │ kobo-highlights-sync │
│  Highlights  │  Kobo Reading   │    (port 8787)       │
│  & Notes     │  Services API   │         │            │
│              │ <─────────────  │         ▼            │
└──────────────┘                 │  Calibre metadata.db │
                                 │         │            │
                                 │         ▼            │
                                 │  Calibre-Web :8083   │
                                 │  (highlights on      │
                                 │   book detail page)  │
                                 └─────────────────────┘
```

## Requirements

- Python 3.9+
- Calibre-Web (linuxserver/calibre-web Docker image recommended)
- Kobo e-reader with WiFi (tested on Kobo Clara Colour, firmware 4.45.x)
- Kobo must be configured to sync with your Calibre-Web instance ([setup guide](https://github.com/janeczku/calibre-web/wiki/Kobo-Integration))

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/jstriblet/kobo-highlights-sync.git
cd kobo-highlights-sync
```

### 2. Start the sync server

```bash
python3 server.py --port 8787 --db /path/to/calibre/metadata.db --verbose
```

Or install as a systemd service:

```bash
# Edit kobo-highlights-sync.service to match your paths
sudo cp kobo-highlights-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kobo-highlights-sync
```

### 3. Patch Calibre-Web

Mount the patched files into your Calibre-Web container. Add these volumes to your `docker-compose.yml`:

```yaml
calibre-web:
  volumes:
    # ... your existing volumes ...
    - /path/to/kobo-highlights-sync/calibre-web-patches/kobo.py:/app/calibre-web/cps/kobo.py:ro
    - /path/to/kobo-highlights-sync/calibre-web-patches/web.py:/app/calibre-web/cps/web.py:ro
    - /path/to/kobo-highlights-sync/calibre-web-patches/detail.html:/app/calibre-web/cps/templates/detail.html:ro
```

#### What the patches do

- **`kobo.py`** — Adds one line to the Kobo init response: redirects `reading_services_host` from `readingservices.kobo.com` to your sync server
- **`web.py`** — Adds a `get_book_highlights()` helper that reads from Calibre's annotations table and passes highlights to the detail template
- **`detail.html`** — Adds a "Highlights" section to the book detail page with styled blockquotes

> **Note:** The patches are based on the linuxserver/calibre-web image. If you use a different version, you may need to adapt `web.py` and `detail.html` to match your Calibre-Web version. `kobo.py` is the most important one — the other two are for display only.

#### Configuring the server address

Edit the `reading_services_host` line in `kobo.py` to match your server's IP and port:

```python
kobo_resources["reading_services_host"] = "http://YOUR_SERVER_IP:8787"
```

Then restart Calibre-Web:

```bash
docker restart calibre-web
```

### 4. Sync your Kobo

Tap **Sync** on your Kobo. On the first sync, the server will receive all your existing highlights. They'll appear on each book's detail page in Calibre-Web.

## How highlights look in Calibre-Web

Highlights appear on the book detail page as yellow-bordered blockquotes. Notes are displayed below the highlighted text.

## Configuration

```
python3 server.py --help

  --port PORT           TCP port (default: 8787)
  --db PATH             Path to Calibre metadata.db
  --annotation-db PATH  Path to Kobo annotation store DB
  --verbose             Enable debug logging
```

## API Endpoints

### Kobo Reading Services (used by Kobo device)

- `POST /api/v3/content/checkforchanges` — Kobo sends book IDs to check for updates
- `GET /api/v3/content/{content_id}/annotations` — Returns stored annotations
- `PATCH /api/v3/content/{content_id}/annotations` — Receives new/updated annotations

### Utility

- `GET /health` — Health check
- `GET /highlights?book_id=N` — Browse highlights by Calibre book ID

## How the protocol works

The Kobo annotation sync protocol (reverse-engineered for this project):

1. Kobo sends `POST /checkforchanges` with a list of book ContentIds and etags
2. Server responds with a flat array of ContentIds that have changed
3. Kobo `GET`s annotations for each changed book
4. If the server returns empty annotations with no etag, the Kobo uploads (`PATCH`) its local annotations
5. Once stored, the server returns annotations with an etag — the Kobo uses `If-None-Match` for efficient caching

**Important:** The ContentId used by the Kobo is the same as the `uuid` field in Calibre's `books` table in `metadata.db`.

## Running tests

```bash
python3 -m pytest -v
```

## Limitations

- Only syncs highlights and notes (not dogears or reading progress)
- The Calibre-Web patches are for the linuxserver/calibre-web Docker image — other installations may need adjustment
- First sync after clearing the annotation store will briefly show empty annotations to the Kobo before the PATCH populates them

## License

MIT
