#!/usr/bin/env bash
# End-to-end demo of the resumable NDJSON ingest flow (断点入库补传).
#
# Prereqs: the service is running, e.g.  docker compose up --build
# Override the target with:  BASE_URL=http://host:8000 ./demo/resumable_upload_demo.sh
#
# The script:
#   1. builds a small UTF-8 NDJSON file (with a multibyte CJK id);
#   2. creates an upload session with fixed total bytes + SHA-256;
#   3. uploads chunks OUT OF ORDER and across line/character boundaries;
#   4. retransmits an identical overlapping chunk (idempotent) and shows a
#      conflicting chunk being rejected while stored bytes stay intact;
#   5. queries missing byte ranges;
#   6. completes the upload, retries completion (same batch), then reads the
#      records back in original line order with pagination.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
UPLOAD_ID="demo-$(date +%s)"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
file="$tmpdir/scans.ndjson"

cat > "$file" <<'JSON'
{"id":"ARR_JFK","local_time":"2024-11-03T01:30:00","timezone":"America/New_York"}
{"id":"DEP_JFK","local_time":"2024-11-03T02:45:00","timezone":"America/New_York"}
{"id":"货物-ORD","local_time":"2024-11-03T04:10:00","timezone":"America/Chicago"}
JSON

TOTAL_BYTES=$(wc -c < "$file" | tr -d ' ')
SHA256=$(sha256sum "$file" | cut -d' ' -f1)
echo "== file: $TOTAL_BYTES bytes, sha256=$SHA256"

echo
echo "== 1) create upload session (total bytes + digest fixed)"
curl -sS -X POST "$BASE_URL/uploads" -H 'Content-Type: application/json' \
  -d "{\"upload_id\":\"$UPLOAD_ID\",\"total_bytes\":$TOTAL_BYTES,\"sha256\":\"$SHA256\"}"
echo

echo
echo "== 2) upload chunks out of order: tail first, then middle, then head"
third=$(( TOTAL_BYTES / 3 ))
tail -c +$(( third + 1 )) "$file" | head -c "$third" > "$tmpdir/c_mid"
tail -c $(( TOTAL_BYTES - 2 * third )) "$file" > "$tmpdir/c_tail"
head -c "$third" "$file" > "$tmpdir/c_head"

put_chunk() {  # $1=start $2=end $3=part-file
  curl -sS -X PUT "$BASE_URL/uploads/$UPLOAD_ID/chunks?start_offset=$1&end_offset=$2" \
    --data-binary "@$3"
  echo
}
put_chunk "$third" "$(( 2 * third ))" "$tmpdir/c_mid"
put_chunk "$(( 2 * third ))" "$TOTAL_BYTES" "$tmpdir/c_tail"
put_chunk 0 "$third" "$tmpdir/c_head"

echo
echo "== 3) missing byte ranges (expect empty)"
curl -sS "$BASE_URL/uploads/$UPLOAD_ID/missing"
echo

echo
echo "== 4a) identical overlap retransmit -> accepted, stored=false"
put_chunk 0 "$third" "$tmpdir/c_head"

echo
echo "== 4b) conflicting overlap -> 409, whole chunk rejected, bytes unchanged"
# Flip the first byte so the overlap differs from the confirmed chunk.
python3 -c 'import pathlib; p=pathlib.Path("'"$tmpdir"'/c_bad"); b=bytearray(pathlib.Path("'"$tmpdir"'/c_head").read_bytes()[:5]); b[0]^=0xFF; p.write_bytes(b)'
curl -sS -o /dev/null -w 'http_status=%{http_code}\n' -X PUT \
  "$BASE_URL/uploads/$UPLOAD_ID/chunks?start_offset=0&end_offset=5" \
  --data-binary "@$tmpdir/c_bad"
curl -sS "$BASE_URL/uploads/$UPLOAD_ID/missing"
echo

echo
echo "== 5) complete upload (gap + digest + every record checked, atomic commit)"
COMPLETE=$(curl -sS -X POST "$BASE_URL/uploads/$UPLOAD_ID/complete")
echo "$COMPLETE"
BATCH_ID=$(echo "$COMPLETE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["batch_id"])')

echo
echo "== 6) retry completion after a 'lost response' -> same batch, one commit"
curl -sS -X POST "$BASE_URL/uploads/$UPLOAD_ID/complete"
echo

echo
echo "== 7) chunks are now immutable"
curl -sS -o /dev/null -w 'http_status=%{http_code}\n' -X PUT \
  "$BASE_URL/uploads/$UPLOAD_ID/chunks?start_offset=0&end_offset=1" \
  --data-binary "@$tmpdir/c_head"

echo
echo "== 8) downstream paged read in original line order (page_size=2)"
curl -sS "$BASE_URL/batches/$BATCH_ID/records?page=1&page_size=2"
echo
curl -sS "$BASE_URL/batches/$BATCH_ID/records?page=2&page_size=2"
echo

echo
echo "demo finished: upload_id=$UPLOAD_ID batch_id=$BATCH_ID"
