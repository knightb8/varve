#!/usr/bin/env python3
"""Render per-project evidence packs from the varve lake for knowledge distillation.

Usage: evidence_pack.py <project-dir-name> <out-dir> [--chunk-kb 120]

Reads the lake through `just sql` (read-only) and writes markdown files
<out-dir>/<project>/pack-NN.md, each holding whole sessions (oldest first) up to
roughly --chunk-kb kilobytes. Per session:

  * header: date, session id, CLI summary
  * the LAST compaction checkpoint (cumulative): technical_details, next_steps
  * every operator prompt (trimmed), corrections marked with the agent text
    they answered
  * tool calls the operator denied
  * ABAP fix loops (objects with >= 3 failed write attempts) and their errors

Stdlib only; DuckDB is called via the CLI so no Python module is needed.
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PROMPT_MAX = 700
PRIOR_MAX = 350


def sql(query: str) -> list[dict]:
    env = dict(os.environ)
    env.setdefault("VARVE_DUCKDB", "duckdb -no-agent")
    out = subprocess.run(["just", "sql", "-json", "-c", query], cwd=REPO, env=env,
                         capture_output=True, text=True, check=True).stdout
    start = out.find("[")
    return json.loads(out[start:]) if start >= 0 else []


def trim(text: str | None, n: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[:n] + f" […+{len(text) - n} chars]"


def esc(s: str) -> str:
    return s.replace("'", "''")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("project")
    ap.add_argument("out")
    ap.add_argument("--chunk-kb", type=int, default=120)
    a = ap.parse_args()
    proj = esc(a.project)
    sess_filter = (f"SELECT session_id FROM sessions WHERE runtime='copilot' "
                   f"AND regexp_extract(cwd, '([^/]+)/?$', 1) = '{proj}'")

    sessions = sql(f"""SELECT session_id, started_at::date AS d, coalesce(cp_summary,'') AS summary, cwd
        FROM sessions WHERE session_id IN ({sess_filter}) ORDER BY started_at""")
    ckpt = {r["session_id"]: r for r in sql(f"""
        SELECT session_id, title, technical_details, next_steps FROM copilot_checkpoints
        WHERE session_id IN ({sess_filter})
        QUALIFY row_number() OVER (PARTITION BY session_id ORDER BY checkpoint_number DESC) = 1""")}
    prompts = sql(f"""
        WITH u AS (
          SELECT m.session_id, m.message_id, m.timestamp, m.content FROM messages m
          WHERE m.session_id IN ({sess_filter}) AND m.role = 'user' AND m.content IS NOT NULL),
        a AS (
          SELECT session_id, timestamp, content FROM messages
          WHERE session_id IN ({sess_filter}) AND role = 'assistant' AND content IS NOT NULL)
        SELECT u.session_id, u.timestamp::varchar AS ts, u.content,
               c.detector, c.features::varchar AS features, a.content AS prior
        FROM u
        LEFT JOIN (SELECT DISTINCT ON (session_id, message_uuid) session_id, message_uuid, detector, features
                   FROM correction_candidates WHERE detector <> 'tool_reject') c
          ON c.session_id = u.session_id AND c.message_uuid = u.message_id
        ASOF LEFT JOIN a ON a.session_id = u.session_id AND a.timestamp < u.timestamp
        ORDER BY u.timestamp""")
    denied = sql(f"""SELECT session_id, started_at::varchar AS ts, tool_name, left(arguments, 300) AS args
        FROM tool_calls WHERE session_id IN ({sess_filter}) AND decision = 'denied' ORDER BY started_at""")
    loops = sql(f"""SELECT session_id, object, count(*) FILTER (WHERE failed) AS fails,
          max(fail_streak) AS streak, count(*) FILTER (WHERE resubmitted_defective) AS resub,
          list_distinct(list(left(first_error, 160)) FILTER (WHERE failed))[1:4] AS errors
        FROM abap_attempts WHERE session_id IN ({sess_filter})
        GROUP BY ALL HAVING count(*) FILTER (WHERE failed) >= 3 ORDER BY fails DESC""")

    by = lambda rows: {s["session_id"]: [r for r in rows if r["session_id"] == s["session_id"]] for s in sessions}
    p_by, d_by, l_by = by(prompts), by(denied), by(loops)

    blocks = []
    for s in sessions:
        sid = s["session_id"]
        out = [f"## Session {s['d']} `{sid[:8]}` — {s['summary']}\n", f"cwd: `{s['cwd']}`\n"]
        if sid in ckpt:
            c = ckpt[sid]
            out.append(f"### Last checkpoint: {c.get('title') or ''}\n")
            out.append("**technical_details**\n\n" + (c.get("technical_details") or "").strip() + "\n")
            out.append("**next_steps**\n\n" + (c.get("next_steps") or "").strip() + "\n")
        if p_by[sid]:
            out.append("### Operator prompts\n")
            for p in p_by[sid]:
                ts = p["ts"][:16]
                if p.get("detector"):
                    out.append(f"- ★ CORRECTION ({p['detector']} {p.get('features') or ''}) {ts}\n"
                               f"  - agent before: {trim(p.get('prior'), PRIOR_MAX)!r}\n"
                               f"  - operator: {trim(p['content'], PROMPT_MAX)!r}")
                else:
                    out.append(f"- {ts} {trim(p['content'], PROMPT_MAX)!r}")
            out.append("")
        if d_by[sid]:
            out.append("### Denied tool calls\n")
            out += [f"- {d['ts'][:16]} {d['tool_name']}: {d['args']}" for d in d_by[sid]]
            out.append("")
        if l_by[sid]:
            out.append("### ABAP fix loops (>= 3 failed writes)\n")
            out += [f"- {l['object']}: {l['fails']} fails, streak {l['streak']}, resubmitted {l['resub']}; "
                    f"errors: {l['errors']}" for l in l_by[sid]]
            out.append("")
        blocks.append("\n".join(out) + "\n")

    dest = Path(a.out) / a.project
    dest.mkdir(parents=True, exist_ok=True)
    for old in dest.glob("pack-*.md"):
        old.unlink()
    chunk, size, n = [], 0, 0
    def flush():
        nonlocal chunk, size, n
        if chunk:
            n += 1
            (dest / f"pack-{n:02d}.md").write_text(f"# Evidence pack {a.project} ({n})\n\n" + "".join(chunk))
            chunk, size = [], 0
    for b in blocks:
        if size and size + len(b) > a.chunk_kb * 1024:
            flush()
        chunk.append(b)
        size += len(b)
    flush()
    print(f"{a.project}: {len(sessions)} sessions -> {n} pack(s) in {dest}", file=sys.stderr)


if __name__ == "__main__":
    main()
