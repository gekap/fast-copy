#!/usr/bin/env python3
"""
FAST COPY GUI — Cross-platform graphical interface.

Launches a local web server and opens a beautiful dark-themed UI in your browser.
Communicates with fast_copy.py backend via HTTP + Server-Sent Events for live progress.

Usage:
  python fast_copy_gui.py              # opens GUI in browser
  python fast_copy_gui.py --port 9090  # custom port

Works on: Windows, Linux, macOS
Requires: fast_copy.py in the same directory
"""

import os
import sys
import json
import time
import shutil
import signal
import socket
import ctypes
import threading
import webbrowser
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

# Import the fast_copy engine
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)
import fast_copy as fc

DEFAULT_PORT = 8787

# ════════════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════════
class AppState:
    """Thread-safe global state for the copy operation."""
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.cancelled = False
        self.phase = ""
        self.progress_pct = 0
        self.bytes_done = 0
        self.bytes_total = 0
        self.files_done = 0
        self.files_total = 0
        self.speed = 0
        self.eta = ""
        self.log = []
        self.result = None  # final result dict when done
        self.scan_result = None

    def reset(self):
        with self.lock:
            self.running = False
            self.cancelled = False
            self.phase = ""
            self.progress_pct = 0
            self.bytes_done = 0
            self.bytes_total = 0
            self.files_done = 0
            self.files_total = 0
            self.speed = 0
            self.eta = ""
            self.log = []
            self.result = None
            self.scan_result = None

    def add_log(self, msg):
        with self.lock:
            self.log.append(msg)
            if len(self.log) > 200:
                self.log = self.log[-200:]

    def to_dict(self):
        with self.lock:
            return {
                "running": self.running,
                "cancelled": self.cancelled,
                "phase": self.phase,
                "progress_pct": self.progress_pct,
                "bytes_done": self.bytes_done,
                "bytes_total": self.bytes_total,
                "files_done": self.files_done,
                "files_total": self.files_total,
                "speed": self.speed,
                "eta": self.eta,
                "log": list(self.log),
                "result": self.result,
                "scan_result": self.scan_result,
            }

state = AppState()


# ════════════════════════════════════════════════════════════════════════════
# COPY WORKER (runs in background thread)
# ════════════════════════════════════════════════════════════════════════════
class GUIProgress:
    """Progress tracker that updates global state instead of printing."""
    def __init__(self, total_bytes, total_files):
        self.total_bytes = total_bytes
        self.total_files = total_files
        self.bytes_done = 0
        self.files_done = 0
        self.lock = threading.Lock()
        self.start = time.time()

    def update(self, nbytes, nfiles=0):
        with self.lock:
            self.bytes_done += nbytes
            self.files_done += nfiles
        self._sync_state()

    def _sync_state(self):
        elapsed = time.time() - self.start
        if elapsed == 0:
            return
        with self.lock:
            bd = self.bytes_done
            fd = self.files_done
        speed = bd / elapsed
        pct = (bd / self.total_bytes * 100) if self.total_bytes else 100
        eta_sec = (self.total_bytes - bd) / speed if speed > 0 else 0

        with state.lock:
            state.progress_pct = min(pct, 100)
            state.bytes_done = bd
            state.bytes_total = self.total_bytes
            state.files_done = fd
            state.files_total = self.total_files
            state.speed = speed
            state.eta = fc.fmt_time(eta_sec)

    def display(self):
        self._sync_state()

    def finish(self):
        with state.lock:
            state.progress_pct = 100
            state.bytes_done = self.total_bytes
            state.files_done = self.total_files


def run_copy(src, dst, buffer_mb, threads, dedup, overwrite, dry_run, excludes):
    """Run the full copy pipeline in a background thread."""
    state.reset()
    state.running = True
    state.phase = "scanning"
    state.add_log(f"Source: {src}")
    state.add_log(f"Destination: {dst}")
    if dry_run:
        state.add_log("MODE: Dry run (no files will be copied)")

    t0 = time.time()

    try:
        buf_size = buffer_mb * 1024 * 1024

        # Phase 1: Scan
        state.phase = "scanning"
        state.add_log("Phase 1 — Scanning source...")
        entries, errors = fc.scan_source(src, dst, excludes)

        if not entries:
            state.add_log("No files found.")
            state.result = {"status": "empty", "message": "No files found in source"}
            state.running = False
            return

        total_size = sum(e.size for e in entries)
        total_files = len(entries)
        state.add_log(f"Found {total_files} files ({fc.fmt_size(total_size)})")

        if errors:
            state.add_log(f"Skipped {len(errors)} unreadable files")

        if state.cancelled:
            state.add_log("Cancelled.")
            state.running = False
            return

        # Phase 2: Dedup
        link_map = {}
        saved_bytes = 0
        copy_entries = entries

        if dedup:
            state.phase = "dedup"
            state.add_log("Phase 2 — Deduplication...")
            copy_entries, link_map, saved_bytes = fc.deduplicate(entries, threads)
            state.add_log(f"Unique: {len(copy_entries)}, Duplicates: {len(link_map)}, "
                          f"Saved: {fc.fmt_size(saved_bytes)}")

        unique_size = sum(e.size for e in copy_entries)

        if state.cancelled:
            state.add_log("Cancelled.")
            state.running = False
            return

        # Phase 2b: Incremental check
        skipped_count = 0
        skipped_bytes = 0

        if not dry_run and not overwrite and os.path.isdir(dst):
            state.phase = "incremental"
            state.add_log("Checking for unchanged files...")
            copy_entries, link_map, skipped_count, skipped_bytes = fc.filter_unchanged(
                copy_entries, link_map, dst, threads
            )
            unique_size = sum(e.size for e in copy_entries)

            if skipped_count:
                state.add_log(f"Skipped {skipped_count} unchanged files ({fc.fmt_size(skipped_bytes)})")

            if not copy_entries and not link_map:
                state.result = {
                    "status": "uptodate",
                    "message": f"All {skipped_count} files already up to date",
                    "time": fc.fmt_time(time.time() - t0),
                }
                state.running = False
                return

        # Phase 3: Space check
        state.phase = "space_check"
        state.add_log("Phase 3 — Checking space...")

        if not dry_run:
            os.makedirs(dst, exist_ok=True)

        try:
            if dry_run and not os.path.isdir(dst):
                state.add_log(f"Destination does not exist yet (will be created)")
            else:
                usage = shutil.disk_usage(dst if os.path.isdir(dst) else os.path.dirname(dst))
                if unique_size > usage.free:
                    state.result = {
                        "status": "no_space",
                        "message": f"Not enough space. Need {fc.fmt_size(unique_size)}, "
                                   f"have {fc.fmt_size(usage.free)}",
                    }
                    state.running = False
                    return
                state.add_log(f"Space OK: {fc.fmt_size(usage.free)} free, "
                              f"need {fc.fmt_size(unique_size)}")
        except OSError:
            state.add_log("Could not check space, proceeding...")

        if state.cancelled:
            state.add_log("Cancelled.")
            state.running = False
            return

        # Phase 4: Physical layout
        state.phase = "mapping"
        state.add_log("Phase 4 — Mapping disk layout...")
        copy_entries = fc.resolve_physical_offsets(copy_entries, threads)
        mapped = sum(1 for e in copy_entries if e.physical_offset > 0)
        state.add_log(f"Mapped {mapped}/{len(copy_entries)} files")

        # ── Dry run exit ──────────────────────────────────────────
        if dry_run:
            small = [e for e in copy_entries if e.size < fc.SMALL_FILE_THRESHOLD]
            large = [e for e in copy_entries if e.size >= fc.SMALL_FILE_THRESHOLD]
            small_sz = sum(e.size for e in small)
            large_sz = sum(e.size for e in large)

            state.add_log("")
            state.add_log("═══ DRY RUN SUMMARY ═══")
            state.add_log(f"Small files (<1MB): {len(small)} files, {fc.fmt_size(small_sz)} → block stream")
            state.add_log(f"Large files (≥1MB): {len(large)} files, {fc.fmt_size(large_sz)} → individual")
            if link_map:
                state.add_log(f"Duplicates: {len(link_map)} files → hard link")
            if skipped_count:
                state.add_log(f"Skipped: {skipped_count} unchanged files ({fc.fmt_size(skipped_bytes)})")
            state.add_log("")
            state.add_log("Top 20 files by disk order:")
            for i, e in enumerate(copy_entries[:20]):
                tag = "BLK" if e.size < fc.SMALL_FILE_THRESHOLD else "IND"
                state.add_log(f"  {i+1:3d}. [{tag}] {fc.fmt_size(e.size):>10s}  {e.rel}")
            if len(copy_entries) > 20:
                state.add_log(f"  ... and {len(copy_entries) - 20} more")

            elapsed = time.time() - t0
            state.result = {
                "status": "dry_run",
                "total_files": total_files,
                "copied_files": len(copy_entries),
                "linked_files": len(link_map),
                "skipped_files": skipped_count,
                "bytes_written": unique_size,
                "bytes_saved": saved_bytes,
                "skipped_bytes": skipped_bytes,
                "time": fc.fmt_time(elapsed),
                "speed": "—",
                "message": (f"Dry run: would copy {fc.fmt_size(unique_size)} "
                            f"({len(copy_entries)} files"
                            + (f", link {len(link_map)} duplicates" if link_map else "")
                            + ")"),
            }
            state.running = False
            return

        if state.cancelled:
            state.add_log("Cancelled.")
            state.running = False
            return

        # Phase 5: Copy
        state.phase = "copying"
        state.add_log("Phase 5 — Copying...")

        progress = GUIProgress(unique_size, len(copy_entries))

        cancel_fn = lambda: state.cancelled
        fc.copy_hybrid(copy_entries, dst, progress, buf_size, cancel_fn)
        progress.finish()

        if state.cancelled:
            state.add_log("Copy cancelled by user.")
            state.result = {
                "status": "cancelled",
                "message": f"Cancelled. {fc.fmt_size(progress.bytes_done)} copied before cancellation.",
                "time": fc.fmt_time(time.time() - t0),
            }
            state.running = False
            return

        # Links
        if link_map:
            state.phase = "linking"
            state.add_log(f"Creating {len(link_map)} links...")
            fc.create_links(link_map, dst)

        elapsed = time.time() - t0
        speed = unique_size / elapsed if elapsed > 0 else 0

        state.result = {
            "status": "done",
            "total_files": total_files,
            "copied_files": len(copy_entries),
            "linked_files": len(link_map),
            "skipped_files": skipped_count,
            "bytes_written": unique_size,
            "bytes_saved": saved_bytes,
            "skipped_bytes": skipped_bytes,
            "time": fc.fmt_time(elapsed),
            "speed": fc.fmt_speed(speed),
            "message": (f"Copied {fc.fmt_size(unique_size)} in {fc.fmt_time(elapsed)} "
                        f"at {fc.fmt_speed(speed)}"),
        }
        state.add_log(f"Done! {state.result['message']}")

    except Exception as e:
        state.result = {"status": "error", "message": str(e)}
        state.add_log(f"Error: {e}")
    finally:
        state.running = False


# ════════════════════════════════════════════════════════════════════════════
# HTTP SERVER
# ════════════════════════════════════════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress console spam

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")

    def _json_response(self, data, code=200):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode())

        elif path == "/api/status":
            self._json_response(state.to_dict())

        elif path == "/api/browse":
            params = urllib.parse.parse_qs(parsed.query)
            dir_path = params.get("path", [""])[0]

            if not dir_path:
                # Return drives on Windows, / on Linux/Mac
                if fc._system == "Windows":
                    drives = []
                    # Use Windows API — more reliable than os.path.exists for USB drives
                    try:
                        bitmask = ctypes.windll.kernel32.GetLogicalDrives()
                        for i in range(26):
                            if bitmask & (1 << i):
                                letter = chr(ord('A') + i)
                                dp = f"{letter}:\\"
                                try:
                                    usage = shutil.disk_usage(dp)
                                    drives.append({
                                        "name": dp,
                                        "path": dp,
                                        "type": "drive",
                                        "free": fc.fmt_size(usage.free),
                                        "total": fc.fmt_size(usage.total),
                                    })
                                except OSError:
                                    drives.append({"name": dp, "path": dp, "type": "drive"})
                    except Exception:
                        # Fallback: scan letters with os.path.exists
                        import string
                        for letter in string.ascii_uppercase:
                            dp = f"{letter}:\\"
                            if os.path.exists(dp):
                                try:
                                    usage = shutil.disk_usage(dp)
                                    drives.append({
                                        "name": dp,
                                        "path": dp,
                                        "type": "drive",
                                        "free": fc.fmt_size(usage.free),
                                        "total": fc.fmt_size(usage.total),
                                    })
                                except OSError:
                                    drives.append({"name": dp, "path": dp, "type": "drive"})
                    self._json_response({"items": drives, "path": ""})
                else:
                    dir_path = "/"

            if dir_path:
                try:
                    items = []
                    # Parent directory
                    parent = str(Path(dir_path).parent)
                    if parent != dir_path:
                        items.append({"name": "..", "path": parent, "type": "dir"})

                    for entry in sorted(os.scandir(dir_path), key=lambda e: e.name.lower()):
                        if entry.name.startswith(".") and entry.name != "..":
                            continue
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                items.append({
                                    "name": entry.name,
                                    "path": os.path.join(dir_path, entry.name),
                                    "type": "dir",
                                })
                        except PermissionError:
                            pass

                    self._json_response({"items": items, "path": dir_path})
                except PermissionError:
                    self._json_response({"error": "Permission denied", "items": [], "path": dir_path})
                except OSError as e:
                    self._json_response({"error": str(e), "items": [], "path": dir_path})

        elif path == "/api/cancel":
            state.cancelled = True
            state.add_log("Cancelling...")
            self._json_response({"ok": True})

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/copy":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))

            if state.running:
                self._json_response({"error": "Copy already in progress"}, 409)
                return

            src = body.get("source", "").strip()
            dst = body.get("destination", "").strip()
            buffer_mb = body.get("buffer", 64)
            threads = body.get("threads", 4)
            dedup = body.get("dedup", True)
            overwrite = body.get("overwrite", False)
            dry_run = body.get("dry_run", False)
            excludes = body.get("excludes", [])

            if not src or not os.path.isdir(src):
                self._json_response({"error": f"Source not found: {src}"}, 400)
                return

            thread = threading.Thread(
                target=run_copy,
                args=(src, dst, buffer_mb, threads, dedup, overwrite, dry_run, excludes),
                daemon=True,
            )
            thread.start()
            self._json_response({"ok": True})

        elif parsed.path == "/api/scan":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len))

            src = body.get("source", "").strip()
            if not src or not os.path.isdir(src):
                self._json_response({"error": f"Source not found: {src}"}, 400)
                return

            try:
                entries, errors = fc.scan_source(src)
                total_size = sum(e.size for e in entries)
                self._json_response({
                    "files": len(entries),
                    "size": total_size,
                    "size_fmt": fc.fmt_size(total_size),
                    "errors": len(errors),
                })
            except Exception as e:
                self._json_response({"error": str(e)}, 500)

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


# ════════════════════════════════════════════════════════════════════════════
# HTML / CSS / JS — EMBEDDED UI
# ════════════════════════════════════════════════════════════════════════════
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fast Copy</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0a0e17;
  --bg2: #111827;
  --bg3: #1a2332;
  --surface: #1e293b;
  --surface2: #273548;
  --border: #334155;
  --border2: #475569;
  --text: #e2e8f0;
  --text2: #94a3b8;
  --text3: #64748b;
  --accent: #22d3ee;
  --accent2: #06b6d4;
  --accent-glow: rgba(34, 211, 238, 0.15);
  --green: #34d399;
  --green-glow: rgba(52, 211, 153, 0.15);
  --red: #f87171;
  --yellow: #fbbf24;
  --orange: #fb923c;
  --font: 'Outfit', -apple-system, sans-serif;
  --mono: 'JetBrains Mono', monospace;
  --radius: 12px;
  --radius-sm: 8px;
}

* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: var(--font);
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  overflow-x: hidden;
}

/* ── Background effect ─────────────────────────────── */
body::before {
  content: '';
  position: fixed;
  top: -50%; left: -50%;
  width: 200%; height: 200%;
  background: radial-gradient(ellipse at 20% 50%, rgba(34, 211, 238, 0.03) 0%, transparent 50%),
              radial-gradient(ellipse at 80% 20%, rgba(52, 211, 153, 0.02) 0%, transparent 50%);
  pointer-events: none;
  z-index: 0;
}

/* ── Layout ────────────────────────────────────────── */
.app {
  position: relative;
  z-index: 1;
  max-width: 860px;
  margin: 0 auto;
  padding: 40px 24px;
}

/* ── Header ────────────────────────────────────────── */
.header {
  text-align: center;
  margin-bottom: 40px;
}
.header h1 {
  font-size: 2.2rem;
  font-weight: 700;
  letter-spacing: -0.5px;
  background: linear-gradient(135deg, var(--accent) 0%, var(--green) 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
.header p {
  color: var(--text3);
  font-size: 0.95rem;
  margin-top: 6px;
  font-weight: 300;
}
.header .version {
  display: inline-block;
  margin-top: 8px;
  font-family: var(--mono);
  font-size: 0.7rem;
  color: var(--text3);
  background: var(--surface);
  padding: 3px 10px;
  border-radius: 20px;
  border: 1px solid var(--border);
}

/* ── Cards ─────────────────────────────────────────── */
.card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 24px;
  margin-bottom: 16px;
  transition: border-color 0.2s;
}
.card:hover { border-color: var(--border2); }
.card-title {
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text3);
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 8px;
}
.card-title .icon {
  width: 18px; height: 18px;
  fill: none;
  stroke: var(--accent);
  stroke-width: 2;
}

/* ── Path inputs ───────────────────────────────────── */
.path-row {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.path-input {
  flex: 1;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 12px 16px;
  color: var(--text);
  font-family: var(--mono);
  font-size: 0.85rem;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}
.path-input:focus {
  border-color: var(--accent2);
  box-shadow: 0 0 0 3px var(--accent-glow);
}
.path-input::placeholder { color: var(--text3); }

.btn-browse {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 0 16px;
  color: var(--text2);
  cursor: pointer;
  font-size: 0.8rem;
  font-family: var(--font);
  font-weight: 500;
  transition: all 0.15s;
  white-space: nowrap;
}
.btn-browse:hover {
  background: var(--surface2);
  color: var(--text);
  border-color: var(--border2);
}

/* ── Options ───────────────────────────────────────── */
.options-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.option {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.15s;
}
.option:hover { border-color: var(--border2); }
.option input[type="checkbox"] {
  appearance: none;
  -webkit-appearance: none;
  width: 18px; height: 18px;
  border: 2px solid var(--border2);
  border-radius: 4px;
  cursor: pointer;
  position: relative;
  flex-shrink: 0;
  transition: all 0.15s;
}
.option input[type="checkbox"]:checked {
  background: var(--accent2);
  border-color: var(--accent2);
}
.option input[type="checkbox"]:checked::after {
  content: '✓';
  position: absolute;
  top: -1px; left: 2px;
  color: var(--bg);
  font-size: 12px;
  font-weight: 700;
}
.option-label {
  font-size: 0.85rem;
  color: var(--text2);
  user-select: none;
}
.option-label strong { color: var(--text); font-weight: 500; }

.buffer-input {
  width: 60px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 4px;
  padding: 4px 8px;
  color: var(--text);
  font-family: var(--mono);
  font-size: 0.8rem;
  text-align: center;
  outline: none;
}
.buffer-input:focus { border-color: var(--accent2); }

/* ── Exclude ───────────────────────────────────────── */
.exclude-input {
  width: 100%;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px 14px;
  color: var(--text);
  font-family: var(--mono);
  font-size: 0.8rem;
  outline: none;
  margin-top: 12px;
}
.exclude-input:focus { border-color: var(--accent2); box-shadow: 0 0 0 3px var(--accent-glow); }
.exclude-hint {
  font-size: 0.72rem;
  color: var(--text3);
  margin-top: 6px;
}

/* ── Action buttons ────────────────────────────────── */
.actions {
  display: flex;
  gap: 12px;
  margin-top: 20px;
}
.btn {
  padding: 14px 32px;
  border: none;
  border-radius: var(--radius-sm);
  font-family: var(--font);
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  gap: 8px;
}
.btn-primary {
  background: linear-gradient(135deg, var(--accent2), #0891b2);
  color: var(--bg);
  flex: 1;
  justify-content: center;
}
.btn-primary:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 20px rgba(6, 182, 212, 0.3);
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
  transform: none;
  box-shadow: none;
}
.btn-cancel {
  background: var(--surface);
  color: var(--red);
  border: 1px solid rgba(248, 113, 113, 0.3);
}
.btn-cancel:hover {
  background: rgba(248, 113, 113, 0.1);
  border-color: var(--red);
}

/* ── Progress ──────────────────────────────────────── */
.progress-section { display: none; }
.progress-section.active { display: block; }

.progress-bar-container {
  width: 100%;
  height: 8px;
  background: var(--bg);
  border-radius: 4px;
  overflow: hidden;
  margin: 16px 0;
}
.progress-bar {
  height: 100%;
  background: linear-gradient(90deg, var(--accent2), var(--green));
  border-radius: 4px;
  transition: width 0.3s ease;
  position: relative;
}
.progress-bar::after {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0; bottom: 0;
  background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.15) 50%, transparent 100%);
  animation: shimmer 2s infinite;
}
@keyframes shimmer {
  0% { transform: translateX(-100%); }
  100% { transform: translateX(100%); }
}

.progress-stats {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-top: 12px;
}
.stat {
  text-align: center;
  padding: 12px;
  background: var(--bg);
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
}
.stat-value {
  font-family: var(--mono);
  font-size: 1.1rem;
  font-weight: 700;
  color: var(--accent);
}
.stat-label {
  font-size: 0.7rem;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 1px;
  margin-top: 4px;
}
.phase-label {
  font-size: 0.8rem;
  color: var(--accent);
  font-weight: 500;
  text-transform: capitalize;
}

/* ── Log ───────────────────────────────────────────── */
.log-box {
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 12px 16px;
  margin-top: 16px;
  max-height: 200px;
  overflow-y: auto;
  font-family: var(--mono);
  font-size: 0.75rem;
  line-height: 1.6;
  color: var(--text2);
}
.log-box::-webkit-scrollbar { width: 6px; }
.log-box::-webkit-scrollbar-track { background: transparent; }
.log-box::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ── Result ────────────────────────────────────────── */
.result-section { display: none; }
.result-section.active { display: block; }

.result-banner {
  padding: 20px;
  border-radius: var(--radius);
  text-align: center;
}
.result-banner.success {
  background: var(--green-glow);
  border: 1px solid rgba(52, 211, 153, 0.3);
}
.result-banner.error {
  background: rgba(248, 113, 113, 0.1);
  border: 1px solid rgba(248, 113, 113, 0.3);
}
.result-banner h3 {
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 6px;
}
.result-banner.success h3 { color: var(--green); }
.result-banner.error h3 { color: var(--red); }
.result-banner p {
  color: var(--text2);
  font-size: 0.85rem;
}

.result-stats {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-top: 16px;
}
.result-stat {
  text-align: center;
  padding: 16px;
  background: var(--bg);
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
}
.result-stat .val {
  font-family: var(--mono);
  font-size: 1.3rem;
  font-weight: 700;
  color: var(--green);
}
.result-stat .lbl {
  font-size: 0.7rem;
  color: var(--text3);
  text-transform: uppercase;
  letter-spacing: 1px;
  margin-top: 4px;
}

.btn-new {
  margin-top: 16px;
  background: var(--surface);
  color: var(--text);
  border: 1px solid var(--border);
  width: 100%;
  justify-content: center;
}
.btn-new:hover { background: var(--surface2); border-color: var(--border2); }

/* ── Donate ───────────────────────────────────────── */
.donate-section {
  margin-top: 24px;
  text-align: center;
}
.donate-section .donate-text {
  font-size: 0.8rem;
  color: var(--text3);
  margin-bottom: 12px;
}
.donate-section .donate-text strong {
  color: var(--text2);
}
.donate-btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  border-radius: var(--radius);
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text);
  font-family: var(--font);
  font-size: 0.85rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}
.donate-btn:hover {
  background: var(--surface2);
  border-color: var(--accent2);
  box-shadow: 0 0 12px var(--accent-glow);
}
.donate-modal-overlay {
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.7);
  backdrop-filter: blur(4px);
  z-index: 200;
  align-items: center;
  justify-content: center;
}
.donate-modal-overlay.active { display: flex; }
.donate-modal {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 28px;
  width: 460px;
  max-width: 90vw;
}
.donate-modal h3 {
  font-size: 1rem;
  font-weight: 600;
  margin-bottom: 16px;
  text-align: center;
  color: var(--text);
}
.donate-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  margin-bottom: 10px;
}
.donate-row .coin-label {
  font-weight: 600;
  font-size: 0.85rem;
  color: var(--accent);
  min-width: 48px;
}
.donate-row .coin-net {
  font-size: 0.7rem;
  color: var(--text3);
  min-width: 52px;
}
.donate-addr {
  flex: 1;
  font-family: var(--mono);
  font-size: 0.7rem;
  color: var(--text2);
  word-break: break-all;
  user-select: all;
}
.btn-copy-addr {
  padding: 4px 10px;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text2);
  font-size: 0.7rem;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.2s ease;
}
.btn-copy-addr:hover {
  background: var(--surface2);
  border-color: var(--accent2);
  color: var(--text);
}
.donate-modal .donate-close {
  display: block;
  margin: 16px auto 0;
  padding: 8px 24px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--text2);
  font-family: var(--font);
  font-size: 0.85rem;
  cursor: pointer;
  transition: all 0.2s ease;
}
.donate-modal .donate-close:hover {
  background: var(--surface2);
  border-color: var(--border2);
}

/* ── Browse modal ──────────────────────────────────── */
.modal-overlay {
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.7);
  backdrop-filter: blur(4px);
  z-index: 100;
  align-items: center;
  justify-content: center;
}
.modal-overlay.active { display: flex; }
.modal {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  width: 500px;
  max-height: 70vh;
  display: flex;
  flex-direction: column;
}
.modal-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.modal-header h3 { font-size: 0.95rem; font-weight: 600; }
.modal-close {
  background: none;
  border: none;
  color: var(--text3);
  cursor: pointer;
  font-size: 1.2rem;
  padding: 4px;
}
.modal-close:hover { color: var(--text); }
.modal-path {
  padding: 10px 20px;
  font-family: var(--mono);
  font-size: 0.75rem;
  color: var(--accent);
  background: var(--bg);
  border-bottom: 1px solid var(--border);
  word-break: break-all;
}
.modal-list {
  flex: 1;
  overflow-y: auto;
  padding: 8px;
}
.modal-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  font-size: 0.85rem;
  transition: background 0.1s;
}
.modal-item:hover { background: var(--surface); }
.modal-item .icon-dir { color: var(--yellow); }
.modal-item .icon-drive { color: var(--accent); }
.modal-footer {
  padding: 12px 20px;
  border-top: 1px solid var(--border);
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}
.modal-footer .btn {
  padding: 8px 20px;
  font-size: 0.8rem;
}

/* ── Responsive ────────────────────────────────────── */
@media (max-width: 640px) {
  .options-grid { grid-template-columns: 1fr; }
  .progress-stats { grid-template-columns: repeat(2, 1fr); }
  .result-stats { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div class="app">
  <!-- Header -->
  <div class="header">
    <h1>⚡ Fast Copy</h1>
    <p>Block-order copy with deduplication</p>
    <span class="version">v2.0 — physical disk order • block stream • dedup</span>
  </div>

  <!-- Source / Destination -->
  <div class="card" id="config-card">
    <div class="card-title">
      <svg class="icon" viewBox="0 0 24 24"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
      PATHS
    </div>
    <div class="path-row">
      <input type="text" class="path-input" id="source" placeholder="Source folder path...">
      <button class="btn-browse" onclick="openBrowser('source')">Browse</button>
    </div>
    <div class="path-row">
      <input type="text" class="path-input" id="destination" placeholder="Destination (USB drive)...">
      <button class="btn-browse" onclick="openBrowser('destination')">Browse</button>
    </div>
  </div>

  <!-- Options -->
  <div class="card">
    <div class="card-title">
      <svg class="icon" viewBox="0 0 24 24"><path d="M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9c.2.47.31.98.33 1.51h.09a2 2 0 1 1 0 4h-.09c-.52.02-1.03.13-1.51.33z"/></svg>
      OPTIONS
    </div>
    <div class="options-grid">
      <label class="option">
        <input type="checkbox" id="opt-dedup" checked>
        <span class="option-label"><strong>Dedup</strong> — skip identical files</span>
      </label>
      <label class="option">
        <input type="checkbox" id="opt-overwrite">
        <span class="option-label"><strong>Overwrite</strong> — force re-copy all</span>
      </label>
      <label class="option">
        <input type="checkbox" id="opt-verify" checked>
        <span class="option-label"><strong>Verify</strong> — check after copy</span>
      </label>
      <label class="option">
        <input type="checkbox" id="opt-dryrun">
        <span class="option-label"><strong>Dry run</strong> — preview only</span>
      </label>
      <label class="option">
        <span class="option-label"><strong>Buffer</strong></span>
        <input type="number" class="buffer-input" id="opt-buffer" value="64" min="1" max="512">
        <span class="option-label">MB</span>
      </label>
      <label class="option">
        <span class="option-label"><strong>Threads</strong></span>
        <input type="number" class="buffer-input" id="opt-threads" value="4" min="1" max="32">
        <span class="option-label"></span>
      </label>
    </div>
    <input type="text" class="exclude-input" id="opt-excludes" placeholder="Exclude: node_modules, .git, __pycache__">
    <div class="exclude-hint">Comma-separated names to exclude from copy</div>
  </div>

  <!-- Actions -->
  <div class="actions" id="action-buttons">
    <button class="btn btn-primary" id="btn-copy" onclick="startCopy()">
      <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3"/></svg>
      Start Copy
    </button>
  </div>

  <!-- Progress -->
  <div class="card progress-section" id="progress-section">
    <div class="card-title">
      <svg class="icon" viewBox="0 0 24 24"><path d="M12 2v4m0 12v4M4.93 4.93l2.83 2.83m8.48 8.48l2.83 2.83M2 12h4m12 0h4M4.93 19.07l2.83-2.83m8.48-8.48l2.83-2.83"/></svg>
      <span class="phase-label" id="phase-label">Initializing...</span>
    </div>
    <div class="progress-bar-container">
      <div class="progress-bar" id="progress-bar" style="width:0%"></div>
    </div>
    <div class="progress-stats">
      <div class="stat">
        <div class="stat-value" id="stat-pct">0%</div>
        <div class="stat-label">Progress</div>
      </div>
      <div class="stat">
        <div class="stat-value" id="stat-speed">—</div>
        <div class="stat-label">Speed</div>
      </div>
      <div class="stat">
        <div class="stat-value" id="stat-files">0</div>
        <div class="stat-label">Files</div>
      </div>
      <div class="stat">
        <div class="stat-value" id="stat-eta">—</div>
        <div class="stat-label">ETA</div>
      </div>
    </div>
    <div class="log-box" id="log-box"></div>
    <div class="actions" style="margin-top:12px">
      <button class="btn btn-cancel" onclick="cancelCopy()">Cancel</button>
    </div>
  </div>

  <!-- Result -->
  <div class="card result-section" id="result-section">
    <div class="result-banner" id="result-banner">
      <h3 id="result-title"></h3>
      <p id="result-message"></p>
    </div>
    <div class="result-stats" id="result-stats"></div>
    <button class="btn btn-new" onclick="resetUI()">New Copy</button>
  </div>

  <!-- Donate -->
  <div class="donate-section">
  <p class="donate-text">If you find <strong>fast-copy</strong> useful, consider supporting development</p>
  <button class="donate-btn" onclick="document.getElementById('donate-modal').classList.add('active')">
    <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>
    Donate
  </button>
  </div>
</div>

<!-- Donate Modal -->
<div class="donate-modal-overlay" id="donate-modal">
  <div class="donate-modal">
    <h3>Support fast-copy</h3>
    <div class="donate-row">
      <span class="coin-label">USDC</span>
      <span class="coin-net">ERC-20</span>
      <span class="donate-addr" id="addr-usdc">0xca8a1223300ab7fff6de983d642b96084305cccb</span>
      <button class="btn-copy-addr" onclick="copyAddr('addr-usdc', this)">Copy</button>
    </div>
    <div class="donate-row">
      <span class="coin-label">ETH</span>
      <span class="coin-net">ERC-20</span>
      <span class="donate-addr" id="addr-eth">0xca8a1223300ab7fff6de983d642b96084305cccb</span>
      <button class="btn-copy-addr" onclick="copyAddr('addr-eth', this)">Copy</button>
    </div>
    <button class="donate-close" onclick="document.getElementById('donate-modal').classList.remove('active')">Close</button>
  </div>
</div>

<!-- Browse Modal -->
<div class="modal-overlay" id="browse-modal">
  <div class="modal">
    <div class="modal-header">
      <h3>Select Folder</h3>
      <button class="modal-close" onclick="closeBrowser()">✕</button>
    </div>
    <div class="modal-path" id="modal-path">/</div>
    <div class="modal-list" id="modal-list"></div>
    <div class="modal-footer">
      <button class="btn" style="background:var(--surface);color:var(--text2);border:1px solid var(--border)" onclick="closeBrowser()">Cancel</button>
      <button class="btn btn-primary" style="flex:none" onclick="selectFolder()">Select This Folder</button>
    </div>
  </div>
</div>

<script>
let pollInterval = null;
let browseTarget = null;
let currentBrowsePath = '';

function fmt(bytes) {
  const units = ['B','KB','MB','GB','TB'];
  let i = 0;
  let n = bytes;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return n.toFixed(1) + ' ' + units[i];
}

function fmtSpeed(bps) { return fmt(bps) + '/s'; }

// ── Copy control ──────────────────────────────────
async function startCopy() {
  const src = document.getElementById('source').value.trim();
  const dst = document.getElementById('destination').value.trim();
  if (!src || !dst) { alert('Please fill in both source and destination paths'); return; }

  const excludeStr = document.getElementById('opt-excludes').value.trim();
  const excludes = excludeStr ? excludeStr.split(',').map(s => s.trim()).filter(Boolean) : [];

  const body = {
    source: src,
    destination: dst,
    buffer: parseInt(document.getElementById('opt-buffer').value) || 64,
    threads: parseInt(document.getElementById('opt-threads').value) || 4,
    dedup: document.getElementById('opt-dedup').checked,
    overwrite: document.getElementById('opt-overwrite').checked,
    dry_run: document.getElementById('opt-dryrun').checked,
    excludes: excludes,
  };

  try {
    const res = await fetch('/api/copy', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }

    document.getElementById('config-card').style.display = 'none';
    document.querySelector('.card:nth-child(3)').style.display = 'none';
    document.getElementById('action-buttons').style.display = 'none';
    document.getElementById('progress-section').classList.add('active');
    document.getElementById('result-section').classList.remove('active');

    startPolling();
  } catch (e) {
    alert('Failed to start copy: ' + e.message);
  }
}

async function cancelCopy() {
  try { await fetch('/api/cancel'); } catch(e) {}
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(pollStatus, 250);
}

async function pollStatus() {
  try {
    const res = await fetch('/api/status');
    const s = await res.json();

    // Update progress
    document.getElementById('progress-bar').style.width = s.progress_pct.toFixed(1) + '%';
    document.getElementById('stat-pct').textContent = s.progress_pct.toFixed(1) + '%';
    document.getElementById('stat-speed').textContent = s.speed > 0 ? fmtSpeed(s.speed) : '—';
    document.getElementById('stat-files').textContent = s.files_done + '/' + s.files_total;
    document.getElementById('stat-eta').textContent = s.eta || '—';

    const phaseNames = {
      scanning: '📂 Scanning...',
      dedup: '🔍 Deduplicating...',
      incremental: '⚡ Checking changes...',
      space_check: '💾 Checking space...',
      mapping: '🗺️ Mapping disk layout...',
      copying: '📦 Copying...',
      linking: '🔗 Creating links...',
    };
    document.getElementById('phase-label').textContent = phaseNames[s.phase] || s.phase;

    // Update log
    const logBox = document.getElementById('log-box');
    logBox.innerHTML = s.log.map(l => '<div>' + escapeHtml(l) + '</div>').join('');
    logBox.scrollTop = logBox.scrollHeight;

    // Check if done
    if (!s.running && s.result) {
      clearInterval(pollInterval);
      pollInterval = null;
      showResult(s.result);
    }
  } catch(e) {}
}

function showResult(result) {
  document.getElementById('progress-section').classList.remove('active');
  document.getElementById('result-section').classList.add('active');

  const banner = document.getElementById('result-banner');
  const title = document.getElementById('result-title');
  const msg = document.getElementById('result-message');
  const stats = document.getElementById('result-stats');

  if (result.status === 'done') {
    banner.className = 'result-banner success';
    title.textContent = '✓ Copy Complete';
    msg.textContent = result.message;
    stats.innerHTML = `
      <div class="result-stat"><div class="val">${result.copied_files + result.linked_files}</div><div class="lbl">Files</div></div>
      <div class="result-stat"><div class="val">${fmt(result.bytes_written)}</div><div class="lbl">Written</div></div>
      <div class="result-stat"><div class="val">${result.time}</div><div class="lbl">Time</div></div>
      ${result.bytes_saved > 0 ? `<div class="result-stat"><div class="val">${fmt(result.bytes_saved)}</div><div class="lbl">Dedup Saved</div></div>` : ''}
      ${result.skipped_files > 0 ? `<div class="result-stat"><div class="val">${result.skipped_files}</div><div class="lbl">Skipped</div></div>` : ''}
      <div class="result-stat"><div class="val">${result.speed}</div><div class="lbl">Speed</div></div>
    `;
  } else if (result.status === 'dry_run') {
    banner.className = 'result-banner success';
    title.textContent = '🔍 Dry Run Complete';
    msg.textContent = result.message;
    stats.innerHTML = `
      <div class="result-stat"><div class="val">${result.copied_files}</div><div class="lbl">To Copy</div></div>
      <div class="result-stat"><div class="val">${fmt(result.bytes_written)}</div><div class="lbl">Data Size</div></div>
      <div class="result-stat"><div class="val">${result.linked_files}</div><div class="lbl">To Link</div></div>
      ${result.bytes_saved > 0 ? `<div class="result-stat"><div class="val">${fmt(result.bytes_saved)}</div><div class="lbl">Dedup Saves</div></div>` : ''}
      ${result.skipped_files > 0 ? `<div class="result-stat"><div class="val">${result.skipped_files}</div><div class="lbl">Unchanged</div></div>` : ''}
      <div class="result-stat"><div class="val">${result.time}</div><div class="lbl">Scan Time</div></div>
    `;
  } else if (result.status === 'uptodate') {
    banner.className = 'result-banner success';
    title.textContent = '✓ Already Up To Date';
    msg.textContent = result.message;
    stats.innerHTML = '';
  } else if (result.status === 'cancelled') {
    banner.className = 'result-banner error';
    title.textContent = '⏹ Cancelled';
    msg.textContent = result.message;
    stats.innerHTML = result.time ? `<div class="result-stat"><div class="val">${result.time}</div><div class="lbl">Elapsed</div></div>` : '';
  } else {
    banner.className = 'result-banner error';
    title.textContent = '✗ Error';
    msg.textContent = result.message;
    stats.innerHTML = '';
  }
}

function resetUI() {
  document.getElementById('config-card').style.display = '';
  document.querySelector('.card:nth-child(3)').style.display = '';
  document.getElementById('action-buttons').style.display = '';
  document.getElementById('progress-section').classList.remove('active');
  document.getElementById('result-section').classList.remove('active');
  document.getElementById('progress-bar').style.width = '0%';
  document.getElementById('log-box').innerHTML = '';
}

// ── Browse modal ──────────────────────────────────
function openBrowser(target) {
  browseTarget = target;
  currentBrowsePath = document.getElementById(target).value || '';
  document.getElementById('browse-modal').classList.add('active');
  loadDirectory(currentBrowsePath);
}

function closeBrowser() {
  document.getElementById('browse-modal').classList.remove('active');
}

function selectFolder() {
  if (browseTarget && currentBrowsePath) {
    document.getElementById(browseTarget).value = currentBrowsePath;
  }
  closeBrowser();
}

async function loadDirectory(path) {
  try {
    const res = await fetch('/api/browse?path=' + encodeURIComponent(path));
    const data = await res.json();
    currentBrowsePath = data.path || path;
    document.getElementById('modal-path').textContent = currentBrowsePath || 'Drives';

    const list = document.getElementById('modal-list');
    list.innerHTML = data.items.map(item => {
      const icon = item.type === 'drive' ? '💾' : (item.name === '..' ? '⬆️' : '📁');
      const extra = item.free ? ` — ${item.free} free` : '';
      return `<div class="modal-item" onclick="loadDirectory('${escapeAttr(item.path)}')">
        <span>${icon}</span>
        <span>${escapeHtml(item.name)}${extra}</span>
      </div>`;
    }).join('');
  } catch(e) {
    console.error(e);
  }
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function escapeAttr(s) {
  return s.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
}

// ── Donate ────────────────────────────────────────
function copyAddr(id, btn) {
  const addr = document.getElementById(id).textContent;
  navigator.clipboard.writeText(addr).then(() => {
    const orig = btn.textContent;
    btn.textContent = 'Copied!';
    btn.style.borderColor = 'var(--green)';
    btn.style.color = 'var(--green)';
    setTimeout(() => { btn.textContent = orig; btn.style.borderColor = ''; btn.style.color = ''; }, 1500);
  });
}
document.getElementById('donate-modal').addEventListener('click', function(e) {
  if (e.target === this) this.classList.remove('active');
});

// Close modal on overlay click
document.getElementById('browse-modal').addEventListener('click', function(e) {
  if (e.target === this) closeBrowser();
});

// Enter key to start
document.addEventListener('keydown', function(e) {
  if (e.key === 'Enter' && !document.getElementById('progress-section').classList.contains('active')) {
    startCopy();
  }
});
</script>
</body>
</html>
"""


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def find_free_port(preferred=DEFAULT_PORT):
    """Find a free port, starting with the preferred one."""
    for port in range(preferred, preferred + 100):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('127.0.0.1', port))
                return port
        except OSError:
            continue
    return None

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fast Copy GUI")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser")
    args = parser.parse_args()

    port = find_free_port(args.port)
    if not port:
        print(f"Could not find a free port near {args.port}")
        sys.exit(1)

    server = HTTPServer(('127.0.0.1', port), Handler)
    url = f"http://127.0.0.1:{port}"

    print(f"")
    print(f"  ⚡ Fast Copy GUI")
    print(f"  ────────────────────────")
    print(f"  Running at: {url}")
    print(f"  Press Ctrl+C to stop")
    print(f"")

    if not args.no_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
