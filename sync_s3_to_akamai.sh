#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────
# BookStore: S3 → Akamai Object Storage (rclone sync)
#
# Syncs all static assets from AWS S3 to Akamai Object Storage.
# Idempotent — only transfers new or changed files.
# Re-executable at any time before cutover.
#
# Prerequisites:
#   - rclone installed (https://rclone.org/install/)
#   - rclone.conf configured (see below)
#
# Usage:
#   ./sync_s3_to_akamai.sh
#   ./sync_s3_to_akamai.sh --dry-run
# ──────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────

# rclone remote names (from rclone.conf)
S3_REMOTE="aws-s3"
AKAMAI_REMOTE="akamai-obj"

# Buckets to sync
BUCKETS=("book-covers" "thumbnails" "react-build")

# Prefix for bucket names
S3_PREFIX="bookstore"
AKAMAI_PREFIX="bookstore"

# Parallelism settings
TRANSFERS=16        # concurrent file transfers
CHECKERS=8          # concurrent integrity checkers
CHUNK_SIZE="64M"    # multipart upload chunk size

LOG_DIR="/var/log/rclone"
DRY_RUN=""

# Parse arguments
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
    echo "=== DRY RUN MODE ==="
fi

mkdir -p "$LOG_DIR"

# ── rclone.conf example ─────────────────────────────────────────────────
#
# [aws-s3]
# type = s3
# provider = AWS
# region = sa-east-1
# access_key_id = ${AWS_ACCESS_KEY}
# secret_access_key = ${AWS_SECRET_KEY}
#
# [akamai-obj]
# type = s3
# provider = Ceph                          # S3-compatible
# endpoint = br-gru-1.linodeobjects.com    # São Paulo region
# access_key_id = ${AKAMAI_ACCESS_KEY}
# secret_access_key = ${AKAMAI_SECRET_KEY}
# acl = public-read
#
# ────────────────────────────────────────────────────────────────────────

echo "Starting S3 → Akamai Object Storage sync"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

# ── Sync each bucket ───────────────────────────────────────────────────

TOTAL_FILES=0
TOTAL_BYTES=0

for BUCKET in "${BUCKETS[@]}"; do
    echo "────────────────────────────────────────"
    echo "Syncing: ${BUCKET}"

    SRC="${S3_REMOTE}:${S3_PREFIX}-${BUCKET}"
    DST="${AKAMAI_REMOTE}:${AKAMAI_PREFIX}-${BUCKET}"

    rclone sync "$SRC" "$DST" \
        --transfers "$TRANSFERS" \
        --checkers "$CHECKERS" \
        --s3-chunk-size "$CHUNK_SIZE" \
        --progress \
        --stats-one-line \
        --log-file "${LOG_DIR}/rclone-${BUCKET}.log" \
        --log-level INFO \
        $DRY_RUN

    # Get post-sync stats
    SIZE_INFO=$(rclone size "$DST" --json 2>/dev/null || echo '{"count":0,"bytes":0}')
    COUNT=$(echo "$SIZE_INFO" | jq -r '.count')
    BYTES=$(echo "$SIZE_INFO" | jq -r '.bytes')
    HUMAN=$(echo "$SIZE_INFO" | jq -r '.bytes' | numfmt --to=iec 2>/dev/null || echo "${BYTES}B")

    echo "  → ${BUCKET}: ${COUNT} files, ${HUMAN}"
    TOTAL_FILES=$((TOTAL_FILES + COUNT))
    TOTAL_BYTES=$((TOTAL_BYTES + BYTES))
done

echo ""
echo "════════════════════════════════════════"
echo "Sync complete: ${TOTAL_FILES} files total"
echo ""

# ── Validate: compare file counts ──────────────────────────────────────

echo "Validating file counts..."
ALL_MATCH=true

for BUCKET in "${BUCKETS[@]}"; do
    S3_COUNT=$(rclone size "${S3_REMOTE}:${S3_PREFIX}-${BUCKET}" --json 2>/dev/null | jq -r '.count')
    AK_COUNT=$(rclone size "${AKAMAI_REMOTE}:${AKAMAI_PREFIX}-${BUCKET}" --json 2>/dev/null | jq -r '.count')

    if [[ "$S3_COUNT" == "$AK_COUNT" ]]; then
        echo "  ✓ ${BUCKET}: S3=${S3_COUNT}, Akamai=${AK_COUNT}"
    else
        echo "  ✗ ${BUCKET}: S3=${S3_COUNT}, Akamai=${AK_COUNT} — MISMATCH"
        ALL_MATCH=false
    fi
done

echo ""
if $ALL_MATCH; then
    echo "✓ All counts match — sync validated"
else
    echo "✗ Count mismatch detected — review before proceeding"
    exit 1
fi

# ── Post-sync notes ────────────────────────────────────────────────────

echo ""
echo "Next steps:"
echo "  1. Update Cloudflare origin: S3 bucket URL → Akamai Object Storage URL"
echo "  2. MongoDB book documents use cdn.bookstore.com.br — no document updates needed"
echo "  3. Keep S3 buckets as fallback until cutover is confirmed"
echo "  4. Re-run this script daily until cutover to keep Akamai current"
