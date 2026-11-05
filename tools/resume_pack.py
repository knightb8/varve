#!/usr/bin/env python3
"""Render a resume pack for the most recent Copilot CLI sessions of a project.

Reads ~/.copilot/session-state directly (todo lists live only in the per-session
session.db, which the lake does not ingest). Output: markdown on stdout.

Usage: resume_pack.py <cwd-substring> [--sessions N]
"""
import argparse
import json
import sqlite3
import subprocess
from pathlib import Path

ROOT = Path.home() / ".copilot" / "session-state"


def yaml_field(text, key):
    for line in text.splitlines():
        if line.startswith(key + ":"):
            return line[len(key) + 1:].strip()
    return ""


def clip(s, n):
    s = (s or "").strip()
    return s if len(s) <= n else s[:n] + f" […{len(s) - n} chars cut]"


def sessions_for(sub):
    out = []
    for d in ROOT.iterdir():
        ws = d / "workspace.yaml"
        if not ws.is_file():
            continue
        t = ws.read_text(errors="replace")
        cwd = yaml_field(t, "cwd")
        if sub.lower() not in cwd.lower():
            continue
        ev = d / "events.jsonl"
        n_user = 0
        if ev.is_file():
            with ev.open(errors="replace") as f:
                n_user = sum(1 for line in f if '"type":"user.message"' in line)
        out.append(dict(id=d.name, dir=d, cwd=cwd, name=yaml_field(t, "name"),
                        updated=yaml_field(t, "updated_at"),
                        created=yaml_field(t, "created_at"), n_user=n_user))
    return sorted(out, key=lambda s: s["updated"], reverse=True)


def todos(d):
    db = d / "session.db"
    if not db.is_file():
        return None
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(todos)")]
        if not cols:
            return None
        rows = [dict(zip(cols, r)) for r in con.execute("SELECT * FROM todos")]
    finally:
        con.close()
    return rows


def events(d):
    ev = d / "events.jsonl"
    if not ev.is_file():
        return []
    out = []
    with ev.open(errors="replace") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def render(s, n_user, n_asst):
    d = s["dir"]
    print(f"## Session {s['id']}")
    print(f"- name: {s['name'] or '(none)'}; cwd: {s['cwd']}")
    print(f"- created {s['created']}, last update {s['updated']}, user messages {s['n_user']}")
    print(f"- resume: `copilot --resume {s['id']}`\n")

    ap = d / "autopilot-objective.json"
    if ap.is_file():
        cur = json.loads(ap.read_text()).get("current", {})
        print("### Autopilot objective")
        print(f"status {cur.get('status')}: {clip(cur.get('objective'), 600)}")
        if cur.get("completionSummary"):
            print("\n" + clip(cur["completionSummary"], 4000))
        print()

    for p in [d / "plan.md"]:
        if p.is_file():
            print(f"### plan.md\n```\n{clip(p.read_text(errors='replace'), 5000)}\n```\n")

    rows = todos(d)
    if rows:
        open_ = [r for r in rows if r.get("status") not in ("done", "completed", "cancelled")]
        print(f"### Todos: {len(rows)} total, {len(open_)} not done")
        for r in open_:
            print(f"- [{r.get('status')}] **{r.get('id')}** {r.get('title')}: "
                  f"{clip(r.get('description'), 1500)} (updated {r.get('updated_at')})")
        print()

    cp = d / "checkpoints"
    if cp.is_dir():
        files = sorted(p for p in cp.iterdir() if p.name != "index.md")
        if files:
            print(f"### Checkpoints ({len(files)}); last: {files[-1].name}")
            print(f"```\n{clip(files[-1].read_text(errors='replace'), 6000)}\n```\n")

    fl = d / "files"
    if fl.is_dir():
        names = sorted(str(p.relative_to(fl)) for p in fl.rglob("*") if p.is_file())
        if names:
            print("### Session files (in " + str(fl) + ")")
            for n in names[:40]:
                print(f"- {n}")
            print()

    ev = events(d)
    users = [e for e in ev if e.get("type") == "user.message"]
    assts = [e for e in ev if e.get("type") == "assistant.message"
             and (e.get("data") or {}).get("content")]
    print(f"### Last {n_user} user messages")
    for e in users[-n_user:]:
        print(f"- {e.get('timestamp', '')[:16]}: {clip(e['data'].get('content'), 1200)}")
    print(f"\n### Last {n_asst} assistant messages")
    for e in assts[-n_asst:]:
        print(f"- {e.get('timestamp', '')[:16]}:\n\n{clip(e['data'].get('content'), 3000)}\n")
    tail = [e.get("type") for e in ev[-15:]]
    if "abort" in tail or any(t and t.endswith("error") for t in tail):
        print(f"\n**Session ended with:** {', '.join(t for t in tail if t)}\n")
    print()


def git_state(cwd):
    p = Path(cwd)
    if not (p / ".git").exists():
        return
    def g(*a):
        r = subprocess.run(["git", "--no-optional-locks", "-C", str(p), *a], capture_output=True, text=True)
        return r.stdout.strip()
    print(f"## Git state of {cwd}")
    print(f"- branch {g('branch', '--show-current')}; last commits:")
    print("```\n" + g("log", "--oneline", "-8") + "\n```")
    st = g("status", "--short")
    print(f"- uncommitted: {len(st.splitlines())} paths")
    if st:
        print("```\n" + "\n".join(st.splitlines()[:30]) + "\n```")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("project")
    ap.add_argument("--sessions", type=int, default=2)
    ap.add_argument("--user", type=int, default=8)
    ap.add_argument("--asst", type=int, default=3)
    a = ap.parse_args()
    ss = [s for s in sessions_for(a.project) if s["n_user"] > 0]
    print(f"# Resume pack: {a.project}\n")
    print("All sessions with this cwd (newest first):")
    for s in ss:
        print(f"- {s['updated'][:16]} {s['id']} ({s['n_user']} prompts) {s['name']}")
    print()
    if ss:
        git_state(ss[0]["cwd"])
    for s in ss[:a.sessions]:
        render(s, a.user, a.asst)


if __name__ == "__main__":
    main()
