#!/usr/bin/env bash
# Emergency backup of the raw GitHub Copilot CLI history plus varve operator data.
#
# Contents of the archive:
#   copilot/            ~/.copilot without locks, logs and live SQLite files
#   copilot/session-store.db, copilot/session-state/<id>/session.db
#                       consistent copies made with `sqlite3 .backup` (WAL included)
#   varve/              attribution.csv, distill/, resume/, baseline-*/ from the varve data dir
#
# Restore: see RESTORE.md inside the archive.
# Usage: backup_copilot.sh [extra_dest_dir ...]
#   The archive is written to $VARVE_DATA/backup and copied to each extra dir.
set -euo pipefail

src="${VARVE_COPILOT_ROOT:-$HOME/.copilot}"
data="${VARVE_DATA:-$HOME/.local/share/varve}"
out_dir="$data/backup"
stamp="$(date +%Y-%m-%d-%H%M)"
name="copilot-history-$stamp"

if pgrep -x copilot >/dev/null 2>&1; then
  echo "backup_copilot: copilot is running; close all sessions first" >&2
  exit 1
fi

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
root="$stage/$name"
mkdir -p "$root/copilot" "$root/varve" "$out_dir"

# Plain files: everything except locks, logs and the live SQLite files.
rsync -a \
  --exclude '*.lock' --exclude '*.hold' --exclude 'logs/' \
  --exclude 'session-store.db*' --exclude 'session.db*' \
  "$src/" "$root/copilot/"

# SQLite: consistent copies through the backup API (folds in the WAL).
sqlite3 -readonly "$src/session-store.db" ".backup '$root/copilot/session-store.db'"
n=0
for db in "$src"/session-state/*/session.db; do
  [ -f "$db" ] || continue
  id="$(basename "$(dirname "$db")")"
  sqlite3 -readonly "$db" ".backup '$root/copilot/session-state/$id/session.db'"
  n=$((n + 1))
done

# varve operator data (small; the lake itself is rebuilt from copilot/).
for p in attribution.csv distill resume baseline-*; do
  [ -e "$data/$p" ] && cp -a "$data/$p" "$root/varve/"
done

cat > "$root/RESTORE.md" <<EOF
# Restore ($stamp)

Made from: $src on $(hostname) ($n per-session todo databases).

1. Close every Copilot CLI session.
2. Copy \`copilot/\` back to \`~/.copilot/\` (rsync -a copilot/ ~/.copilot/).
   Keep an existing ~/.copilot aside first if it holds newer sessions.
3. Copy \`varve/*\` to \`~/.local/share/varve/\`.
4. Rebuild the lake: \`cd ~/code/varve && VARVE_DUCKDB=duckdb just backfill\`
   (or \`just ingest\` on top of an existing lake).
5. Resume notes: \`~/.local/share/varve/resume/INDEX.md\`.
   \`copilot --resume <session-id>\` reopens a session from the restored history.
EOF

archive="$out_dir/$name.tar.zst"
tar -C "$stage" -cf - "$name" | zstd -q -T0 -19 -o "$archive"
(cd "$out_dir" && sha256sum "$name.tar.zst" > "$name.tar.zst.sha256")
echo "backup_copilot: $archive ($(du -h "$archive" | cut -f1))"

for dest in "$@"; do
  mkdir -p "$dest"
  cp "$archive" "$archive.sha256" "$dest/"
  (cd "$dest" && sha256sum -c "$name.tar.zst.sha256" >/dev/null) \
    && echo "backup_copilot: copied and verified -> $dest"
done
