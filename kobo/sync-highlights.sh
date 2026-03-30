#!/bin/sh
# Kobo Highlights Sync — runs on Kobo Clara Color
# Reads highlights from KoboReader.sqlite and POSTs to ThinkCenter

SYNC_SERVER="http://192.168.1.195:8787"
KOBO_DB="/mnt/onboard/.kobo/KoboReader.sqlite"
TMPFILE="/tmp/highlights-payload.json"
DEVICE_ID="kobo-clara-color"

# Check WiFi connectivity
ping -c 1 -W 2 192.168.1.195 > /dev/null 2>&1
if [ $? -ne 0 ]; then
    fbink -pm "No connection to server"
    exit 1
fi

fbink -pm "Syncing highlights..."

# Extract highlights with book metadata
sqlite3 -json "$KOBO_DB" "
    SELECT
        b.BookmarkID as bookmark_id,
        b.Text as text,
        b.Annotation as annotation,
        b.DateCreated as date_created,
        b.ChapterProgress as chapter_progress,
        b.ContentID as content_id,
        b.StartContainerPath as start_path,
        b.EndContainerPath as end_path,
        c.BookTitle as book_title,
        c.Attribution as book_author,
        c.BookID as volume_id
    FROM Bookmark b
    LEFT JOIN content c ON c.ContentID = b.VolumeID
    WHERE b.Hidden = 0
        AND b.Text IS NOT NULL
        AND b.Text != ''
    ORDER BY b.DateCreated DESC;
" > /tmp/highlights-raw.json 2>/dev/null

if [ ! -s /tmp/highlights-raw.json ]; then
    fbink -pm "No highlights found"
    exit 0
fi

# Build payload
cat > "$TMPFILE" << PAYLOAD_EOF
{"device_id":"$DEVICE_ID","highlights":$(cat /tmp/highlights-raw.json)}
PAYLOAD_EOF

# POST to server
RESPONSE=$(wget -q -O - \
    --header="Content-Type: application/json" \
    --post-file="$TMPFILE" \
    "$SYNC_SERVER/sync" 2>&1)

if [ $? -eq 0 ]; then
    fbink -pm "Highlights synced!"
else
    fbink -pm "Sync failed: $RESPONSE"
fi

rm -f /tmp/highlights-raw.json "$TMPFILE"
