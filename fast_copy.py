#!/usr/bin/env python3
# Copyright 2026 George Kapellakis
# Licensed under the Apache License, Version 2.0
# See LICENSE file for details.
"""
FAST BLOCK-ORDER COPY — Copies files and folders at maximum sequential disk speed.

Features:
  • Reads files in PHYSICAL disk order (eliminates random seeks)
  • Pre-flight space check (compares source size vs USB free space)
  • Content-aware deduplication (hashes files, copies each unique file once,
    hard-links duplicates — like Dell's backup dedup)
  • Cross-run dedup database (SQLite cache at destination — skips re-hashing
    unchanged files, detects content already on destination from prior runs)
  • Strong hashing (xxh128 / SHA-256 fallback) for collision safety
  • Large I/O buffers (64MB default)
  • Post-copy verification

Usage:
  python fast_copy.py <source> <destination>

  Source can be a folder, a single file, or a glob/wildcard pattern.

Examples:
  python fast_copy.py "C:\\Projects" "E:\\Backup\\Projects"     # folder
  python fast_copy.py /home/user/data /media/usb/data           # folder
  python fast_copy.py ~/Downloads/file.iso /mnt/usb/            # single file
  python fast_copy.py "~/Downloads/*.zip" /mnt/usb/zips/        # wildcard
  python fast_copy.py "/data/logs/*.log" /mnt/usb/logs/         # glob
  python fast_copy.py /data /mnt/usb --no-dedup                 # skip dedup
  python fast_copy.py /data /mnt/usb --force                    # skip space check

Options:
  --buffer MB     Read/write buffer size in MB (default: 64)
  --threads N     Threads for hashing & layout resolution (default: 4)
  --dry-run       Show copy plan without copying
  --no-verify     Skip post-copy verification
  --no-dedup      Disable deduplication (copy all files even if identical)
  --no-cache      Disable persistent hash cache (cross-run dedup database)
  --force         Skip space check and copy anyway

Build standalone executable:
  pip install pyinstaller
  pyinstaller --onefile --name fast_copy fast_copy.py
"""

import os
import sys
import stat
import time
import glob as globmod
import struct
import ctypes
import shutil
import hashlib
import tarfile
import io
import json
import sqlite3
import argparse
import platform
import threading
from pathlib import Path
from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════
DEFAULT_BUFFER_MB = 64
DEFAULT_THREADS = 4
HASH_CHUNK = 1048571            # ~1MB chunks for hashing (prime for alignment)
HASH_ALGO = "xxh128"            # try xxhash first, fallback to sha256

FileEntry = namedtuple("FileEntry", ["src", "rel", "size", "physical_offset", "content_hash"])

# ════════════════════════════════════════════════════════════════════════════
# TERMINAL OUTPUT
# ════════════════════════════════════════════════════════════════════════════
_is_tty = sys.stdout.isatty()

class C:
    GREEN  = "\033[92m" if _is_tty else ""
    YELLOW = "\033[93m" if _is_tty else ""
    RED    = "\033[91m" if _is_tty else ""
    CYAN   = "\033[96m" if _is_tty else ""
    BOLD   = "\033[1m"  if _is_tty else ""
    DIM    = "\033[2m"  if _is_tty else ""
    RESET  = "\033[0m"  if _is_tty else ""

def fmt_size(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

def fmt_speed(bps):
    return f"{fmt_size(bps)}/s"

def fmt_time(s):
    if s < 60:
        return f"{s:.1f}s"
    m, s = divmod(int(s), 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"

def fmt_pct(a, b):
    if b == 0:
        return "0%"
    return f"{a / b * 100:.1f}%"

def banner(msg):
    print(f"\n{C.BOLD}{C.CYAN}{'─'*60}")
    print(f"  {msg}")
    print(f"{'─'*60}{C.RESET}\n")


# ════════════════════════════════════════════════════════════════════════════
# HASHING — use xxhash if available (10x faster), fallback to sha256
# ════════════════════════════════════════════════════════════════════════════
try:
    import xxhash
    def new_hasher():
        return xxhash.xxh128()
    _hash_name = "xxh128"
except ImportError:
    def new_hasher():
        return hashlib.sha256()
    _hash_name = "sha256"


def hash_file(filepath, buf_size=HASH_CHUNK):
    """Hash file contents. Returns hex digest string."""
    h = new_hasher()
    try:
        with open(filepath, "rb") as f:
            chunk = f.read(buf_size)
            if not chunk:
                return "e3b0c44298fc1c149afbf4c8996fb924"  # empty file sentinel
            while chunk:
                h.update(chunk)
                chunk = f.read(buf_size)
        return h.hexdigest()
    except OSError:
        return None


# ════════════════════════════════════════════════════════════════════════════
# DEDUP DATABASE — persistent hash cache across runs
# ════════════════════════════════════════════════════════════════════════════
DEDUP_DB_NAME = ".fast_copy_dedup.db"


def _find_mount_point(path):
    """Walk up from path to find the filesystem mount point."""
    path = os.path.realpath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


class DedupDB:
    """
    SQLite-backed hash cache stored at the mount/drive root.
    Shared across all destinations on the same drive.

    Two tables:
      source_cache  — keyed on (source rel_path, size, mtime_ns)
                      Speeds up hashing: same source file → same hash
                      regardless of which destination subfolder you copy to.
      dest_files    — keyed on mount-relative path
                      Tracks what's actually on the drive for cross-run dedup.
    """

    def __init__(self, dst_root):
        self.dst_root = os.path.realpath(dst_root)
        self.mount = _find_mount_point(dst_root)
        db_path = os.path.join(self.mount, DEDUP_DB_NAME)
        self.db_path = db_path
        # Prefix to convert dest-relative → mount-relative
        self._prefix = os.path.relpath(self.dst_root, self.mount)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        # Restrict DB file permissions to owner-only (contains file paths/hashes)
        try:
            os.chmod(db_path, 0o600)
        except OSError:
            pass
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")  # cache is rebuildable
        self.conn.execute("PRAGMA user_version=4718")  # schema v2
        self.lock = threading.Lock()
        self._init_schema()

    def _mount_rel(self, rel_path):
        """Convert destination-relative path to mount-relative path."""
        return os.path.join(self._prefix, rel_path)

    def _init_schema(self):
        c = self.conn.cursor()
        # Source hash cache — shared across all destination folders
        c.execute("""
            CREATE TABLE IF NOT EXISTS source_cache (
                rel_path    TEXT NOT NULL,
                size        INTEGER NOT NULL,
                mtime_ns    INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                hash_algo   TEXT NOT NULL,
                PRIMARY KEY (rel_path, hash_algo)
            )
        """)
        # Destination file index — tracks files on the drive
        c.execute("""
            CREATE TABLE IF NOT EXISTS dest_files (
                mount_rel   TEXT PRIMARY KEY,
                size        INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                hash_algo   TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_dest_hash
            ON dest_files (content_hash)
        """)
        # Migrate old single-table schema if present
        try:
            c.execute("SELECT 1 FROM file_hashes LIMIT 1")
            c.execute("DROP TABLE file_hashes")
        except sqlite3.OperationalError:
            pass
        self.conn.commit()

    # ── Source cache (hash speedup) ───────────────────────────────

    def lookup(self, rel_path, size, mtime_ns):
        """Return cached hash if source file size+mtime match, else None."""
        with self.lock:
            c = self.conn.cursor()
            c.execute(
                "SELECT content_hash FROM source_cache "
                "WHERE rel_path = ? AND size = ? AND mtime_ns = ? AND hash_algo = ?",
                (rel_path, size, mtime_ns, _hash_name),
            )
            row = c.fetchone()
            return row[0] if row else None

    def store_source_batch(self, rows):
        """Cache source hashes. rows = list of (rel_path, size, mtime_ns, hash)."""
        with self.lock:
            self.conn.executemany(
                "INSERT OR REPLACE INTO source_cache "
                "(rel_path, size, mtime_ns, content_hash, hash_algo) "
                "VALUES (?, ?, ?, ?, ?)",
                [(r[0], r[1], r[2], r[3], _hash_name) for r in rows],
            )
            self.conn.commit()

    # ── Destination index (cross-run dedup) ───────────────────────

    def store_dest_batch(self, rows):
        """Record files on the drive. rows = list of (rel_path, size, hash).
        rel_path is destination-relative; stored as mount-relative."""
        with self.lock:
            self.conn.executemany(
                "INSERT OR REPLACE INTO dest_files "
                "(mount_rel, size, content_hash, hash_algo) "
                "VALUES (?, ?, ?, ?)",
                [(self._mount_rel(r[0]), r[1], r[2], _hash_name) for r in rows],
            )
            self.conn.commit()

    def lookup_by_hash(self, content_hash):
        """Find files on this drive with this hash.
        Returns list of (mount_rel_path, size)."""
        with self.lock:
            c = self.conn.cursor()
            c.execute(
                "SELECT mount_rel, size FROM dest_files "
                "WHERE content_hash = ? AND hash_algo = ?",
                (content_hash, _hash_name),
            )
            return c.fetchall()

    def close(self):
        self.conn.commit()
        self.conn.close()


# ════════════════════════════════════════════════════════════════════════════
# SPACE CHECK
# ════════════════════════════════════════════════════════════════════════════
def check_destination_space(dst, required_bytes, force=False):
    """
    Check if destination has enough free space.
    Creates the destination directory if needed to query its filesystem.
    Returns True if OK to proceed, False to abort.
    """
    # Create dst if it doesn't exist so we can stat its filesystem
    os.makedirs(dst, exist_ok=True)

    try:
        usage = shutil.disk_usage(dst)
        free = usage.free
        total = usage.total
    except OSError as e:
        print(f"  {C.YELLOW}Warning: Could not check free space: {e}{C.RESET}")
        if force:
            print(f"  {C.YELLOW}--force: proceeding anyway{C.RESET}")
            return True
        print(f"  Use --force to skip this check.")
        return False

    pct_used = (total - free) / total * 100 if total > 0 else 0

    print(f"  Destination disk:")
    print(f"    Total:     {C.BOLD}{fmt_size(total)}{C.RESET}")
    print(f"    Free:      {C.BOLD}{fmt_size(free)}{C.RESET} ({100 - pct_used:.1f}% free)")
    print(f"    Required:  {C.BOLD}{fmt_size(required_bytes)}{C.RESET}")

    if required_bytes > free:
        shortfall = required_bytes - free
        print(f"\n  {C.RED}✗ NOT ENOUGH SPACE — need {fmt_size(shortfall)} more{C.RESET}")
        print(f"  {C.RED}  Source: {fmt_size(required_bytes)} > Free: {fmt_size(free)}{C.RESET}")
        if force:
            print(f"\n  {C.YELLOW}--force: proceeding anyway (copy may fail mid-way){C.RESET}")
            return True
        print(f"\n  Use --force to attempt anyway, or free up space on the destination.")
        return False

    headroom = free - required_bytes
    print(f"    Headroom:  {C.GREEN}{fmt_size(headroom)}{C.RESET}")
    print(f"\n  {C.GREEN}✓ Enough space{C.RESET}")
    return True


# ════════════════════════════════════════════════════════════════════════════
# PHYSICAL OFFSET DETECTION — WINDOWS
# ════════════════════════════════════════════════════════════════════════════
def get_physical_offset_windows(filepath):
    """Use FSCTL_GET_RETRIEVAL_POINTERS for starting LCN."""
    try:
        import ctypes.wintypes as wt

        GENERIC_READ = 0x80000000
        FILE_SHARE_READ = 1
        FILE_SHARE_WRITE = 2
        OPEN_EXISTING = 3
        FSCTL_GET_RETRIEVAL_POINTERS = 0x00090073

        kernel32 = ctypes.windll.kernel32
        CreateFileW = kernel32.CreateFileW
        CreateFileW.restype = wt.HANDLE
        DeviceIoControl = kernel32.DeviceIoControl
        CloseHandle = kernel32.CloseHandle

        handle = CreateFileW(
            str(filepath), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None, OPEN_EXISTING, 0, None,
        )
        INVALID = wt.HANDLE(-1).value
        if handle == INVALID:
            return 0

        try:
            in_buf = struct.pack("<Q", 0)
            out_size = 16 + 16 * 64
            out_buf = ctypes.create_string_buffer(out_size)
            bytes_returned = wt.DWORD(0)

            ok = DeviceIoControl(
                handle, FSCTL_GET_RETRIEVAL_POINTERS,
                in_buf, len(in_buf),
                out_buf, out_size,
                ctypes.byref(bytes_returned), None,
            )
            if not ok:
                return 0

            raw = out_buf.raw[:bytes_returned.value]
            if len(raw) < 32:
                return 0

            extent_count = struct.unpack_from("<I", raw, 0)[0]
            if extent_count == 0:
                return 0

            first_lcn = struct.unpack_from("<q", raw, 24)[0]
            return first_lcn if first_lcn >= 0 else 0
        finally:
            CloseHandle(handle)
    except Exception:
        return 0


# ════════════════════════════════════════════════════════════════════════════
# PHYSICAL OFFSET DETECTION — LINUX
# ════════════════════════════════════════════════════════════════════════════
def get_physical_offset_linux(filepath):
    """Use FIEMAP ioctl for physical block offset."""
    try:
        import fcntl

        FS_IOC_FIEMAP = 0xC020660B
        FIEMAP_SIZE = 32
        EXTENT_SIZE = 56
        FIEMAP_FLAG_SYNC = 0x00000001

        fd = os.open(filepath, os.O_RDONLY)
        try:
            fiemap = bytearray(FIEMAP_SIZE + EXTENT_SIZE)
            struct.pack_into("<Q", fiemap, 0, 0)
            struct.pack_into("<Q", fiemap, 8, 0xFFFFFFFFFFFFFFFF)
            struct.pack_into("<I", fiemap, 16, FIEMAP_FLAG_SYNC)
            struct.pack_into("<I", fiemap, 24, 1)

            fcntl.ioctl(fd, FS_IOC_FIEMAP, fiemap)

            mapped = struct.unpack_from("<I", fiemap, 20)[0]
            if mapped == 0:
                return 0

            physical = struct.unpack_from("<Q", fiemap, FIEMAP_SIZE + 8)[0]
            return physical
        finally:
            os.close(fd)
    except Exception:
        try:
            import fcntl
            FIBMAP = 1
            fd = os.open(filepath, os.O_RDONLY)
            try:
                block = struct.pack("<I", 0)
                result = fcntl.ioctl(fd, FIBMAP, block)
                return struct.unpack("<I", result)[0]
            finally:
                os.close(fd)
        except Exception:
            return 0


# ════════════════════════════════════════════════════════════════════════════
# PHYSICAL OFFSET DETECTION — MACOS (heuristic)
# ════════════════════════════════════════════════════════════════════════════
def get_physical_offset_macos(filepath):
    """inode number as proxy for physical position."""
    try:
        return os.stat(filepath).st_ino
    except Exception:
        return 0


# ════════════════════════════════════════════════════════════════════════════
# UNIFIED OFFSET GETTER
# ════════════════════════════════════════════════════════════════════════════
_system = platform.system()

def get_physical_offset(filepath):
    if _system == "Linux":
        return get_physical_offset_linux(filepath)
    elif _system == "Windows":
        return get_physical_offset_windows(filepath)
    elif _system == "Darwin":
        return get_physical_offset_macos(filepath)
    return 0


# ════════════════════════════════════════════════════════════════════════════
# FOLDER SCANNER
# ════════════════════════════════════════════════════════════════════════════
def scan_source(src_root, dst_root=None, excludes=None):
    """Walk source tree, catalog all files.
    Follows symlinks and junctions (common on Windows with OneDrive/shell folders).
    Skips the destination directory if it's inside the source (prevents infinite loop).
    """
    print(f"  {C.DIM}Scanning files...{C.RESET}", end="", flush=True)

    entries = []
    errors = []
    dir_errors = []
    scan_count = 0
    skipped_dst = False
    visited_real = set()  # avoid infinite loops from circular symlinks

    # Resolve destination path to detect overlap
    dst_real = os.path.realpath(dst_root) if dst_root else None
    src_real = os.path.realpath(src_root)

    # Check if destination is inside source
    if dst_real and dst_real.startswith(src_real + os.sep):
        print(f"\n  {C.YELLOW}Warning: Destination is inside source — it will be excluded{C.RESET}")

    # Compile exclude patterns
    exclude_names = {TAR_BUNDLE_NAME, DEDUP_DB_NAME}  # skip our own files
    if excludes:
        for ex in excludes:
            exclude_names.add(ex)

    def on_walk_error(err):
        """Called by os.walk when it can't list a directory."""
        dir_errors.append((err.filename, str(err)))

    # followlinks=True so we traverse Windows junctions & symlinks
    for root, dirs, files in os.walk(src_root, followlinks=True, onerror=on_walk_error):
        # Circular symlink protection
        try:
            real = os.path.realpath(root)
            if real in visited_real:
                dirs.clear()  # don't descend further
                continue
            visited_real.add(real)
        except OSError:
            pass

        # Skip destination directory if inside source
        if dst_real:
            real_root = os.path.realpath(root)
            dirs_to_remove = []
            for d in dirs:
                dir_real = os.path.realpath(os.path.join(root, d))
                if dir_real == dst_real or dst_real.startswith(dir_real + os.sep):
                    dirs_to_remove.append(d)
                    skipped_dst = True
            for d in dirs_to_remove:
                dirs.remove(d)

        for fname in files:
            # Skip excluded files
            if fname in exclude_names:
                continue

            src_path = os.path.join(root, fname)
            rel_path = os.path.relpath(src_path, src_root)
            try:
                sz = os.path.getsize(src_path)
                entries.append(FileEntry(
                    src=src_path, rel=rel_path, size=sz,
                    physical_offset=0, content_hash=None,
                ))
                scan_count += 1
                if scan_count % 1000 == 0:
                    print(f"\r  {C.DIM}Scanning... {scan_count} files{C.RESET}", end="", flush=True)
            except OSError as e:
                errors.append((src_path, str(e)))

    print(f"\r  {C.GREEN}Found {len(entries)} files{C.RESET}                    ")

    if skipped_dst:
        print(f"  {C.YELLOW}Excluded destination directory from scan{C.RESET}")

    # Show directory access errors (common cause of "0 files found")
    if dir_errors:
        print(f"  {C.YELLOW}Could not access {len(dir_errors)} directories:{C.RESET}")
        for path, err in dir_errors[:10]:
            print(f"    {C.YELLOW}→ {path}{C.RESET}")
            print(f"      {C.DIM}{err}{C.RESET}")
        if len(dir_errors) > 10:
            print(f"    ... and {len(dir_errors) - 10} more")

    # Show file read errors
    if errors:
        print(f"  {C.YELLOW}Skipped {len(errors)} unreadable files:{C.RESET}")
        for path, err in errors[:10]:
            print(f"    {C.YELLOW}→ {path}{C.RESET}")
            print(f"      {C.DIM}{err}{C.RESET}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more")

    # Hint if nothing found
    if not entries and not errors and not dir_errors:
        print(f"  {C.YELLOW}The directory appears to be empty.{C.RESET}")
        if _system == "Windows":
            print(f"  {C.YELLOW}Tip: Your Documents folder may be redirected to OneDrive.{C.RESET}")
            print(f"  {C.YELLOW}     Check: OneDrive\\Documents or run in PowerShell:{C.RESET}")
            print(f"  {C.DIM}     (New-Object -ComObject Shell.Application)"
                  f".NameSpace('shell:Personal').Self.Path{C.RESET}")

    return entries, errors


def resolve_physical_offsets(entries, threads=DEFAULT_THREADS):
    """Query physical offsets in parallel, return entries sorted by disk position."""
    print(f"  {C.DIM}Reading disk layout ({_system})...{C.RESET}", end="", flush=True)

    offsets = [0] * len(entries)

    def get_offset(idx):
        offsets[idx] = get_physical_offset(entries[idx].src)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(get_offset, i) for i in range(len(entries))]
        done = 0
        for f in as_completed(futures):
            f.result()
            done += 1
            if done % 500 == 0:
                print(f"\r  {C.DIM}Reading disk layout... {done}/{len(entries)}{C.RESET}",
                      end="", flush=True)

    new_entries = [
        FileEntry(e.src, e.rel, e.size, offsets[i], e.content_hash)
        for i, e in enumerate(entries)
    ]
    new_entries.sort(key=lambda e: e.physical_offset)

    has_offset = sum(1 for e in new_entries if e.physical_offset > 0)
    print(f"\r  {C.GREEN}Disk layout resolved: {has_offset}/{len(entries)} files mapped{C.RESET}          ")

    if has_offset == 0:
        print(f"  {C.YELLOW}Could not map physical layout — falling back to size-sorted order.{C.RESET}")
        new_entries.sort(key=lambda e: e.size, reverse=True)
    elif has_offset < len(entries) * 0.5:
        print(f"  {C.YELLOW}Partial mapping — unmapped files appended at end.{C.RESET}")

    return new_entries


# ════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ════════════════════════════════════════════════════════════════════════════
def deduplicate(entries, threads=DEFAULT_THREADS, dedup_db=None):
    """
    Content-aware deduplication:
      1. Hash ALL files (using cache when available)
      2. Group by (size, hash) — same-size pre-filter for within-run dedup
      3. Cross-run dedup: check if drive already has matching content
      4. Return (unique_entries, link_map)
    """
    print(f"  Using hash: {C.BOLD}{_hash_name}{C.RESET}")
    if dedup_db:
        print(f"  {C.DIM}Hash cache: enabled (cross-run dedup){C.RESET}")

    # ── Step 1: Hash ALL files (cache-aware) ──────────────────────────
    total = len(entries)
    print(f"  Hashing {total} files...")

    hashes = [None] * total
    cache_hits = [0]
    new_hashes = []  # (rel, size, mtime_ns, hash) for source cache
    done_count = [0]
    lock = threading.Lock()

    def do_hash(idx):
        entry = entries[idx]
        # Use full source path as cache key to avoid collisions when
        # different files share the same basename (e.g. single-file mode)
        cache_key = entry.src
        # Try cache first
        if dedup_db:
            try:
                mtime_ns = os.stat(entry.src).st_mtime_ns
            except OSError:
                mtime_ns = 0
            cached = dedup_db.lookup(cache_key, entry.size, mtime_ns)
            if cached:
                hashes[idx] = cached
                with lock:
                    cache_hits[0] += 1
                return
        # Cache miss — hash the file
        h = hash_file(entry.src)
        hashes[idx] = h
        if dedup_db and h is not None:
            try:
                mtime_ns = os.stat(entry.src).st_mtime_ns
            except OSError:
                mtime_ns = 0
            with lock:
                new_hashes.append((cache_key, entry.size, mtime_ns, h))

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(do_hash, i) for i in range(total)]
        for f in as_completed(futures):
            f.result()
            with lock:
                done_count[0] += 1
                if done_count[0] % 200 == 0:
                    print(f"\r  {C.DIM}Hashing... {done_count[0]}/{total}{C.RESET}",
                          end="", flush=True)

    # Store newly computed hashes in source cache
    if dedup_db and new_hashes:
        dedup_db.store_source_batch(new_hashes)

    if cache_hits[0] > 0:
        print(f"\r  {C.GREEN}Cache: {cache_hits[0]}/{total} hashes from DB "
              f"({total - cache_hits[0]} computed){C.RESET}          ")

    # Update all entries with hashes
    hashed_entries = [
        FileEntry(e.src, e.rel, e.size, e.physical_offset, hashes[i])
        for i, e in enumerate(entries)
    ]

    # ── Step 2: Group by (size, hash) to find duplicates ──────────────
    hash_groups = defaultdict(list)
    unique_entries = []

    for e in hashed_entries:
        if e.content_hash is not None:
            key = (e.size, e.content_hash)
            hash_groups[key].append(e)
        else:
            # Couldn't hash → treat as unique
            unique_entries.append(e)

    link_map = {}       # duplicate_rel → canonical_rel or ("__abs__", path)
    saved_bytes = 0
    crossrun_count = 0
    crossrun_bytes = 0
    crossrun_sources = defaultdict(int)  # folder → file count

    for key, group in hash_groups.items():
        canonical = group[0]

        # ── Cross-run dedup: check if drive already has this content ──
        if dedup_db:
            dst_matches = dedup_db.lookup_by_hash(key[1])  # key[1] = content_hash
            for mount_rel, dst_size in dst_matches:
                # mount_rel is relative to mount point, build full path
                full_path = os.path.join(dedup_db.mount, mount_rel)
                if dst_size == key[0] and os.path.isfile(full_path):
                    # Drive already has a file with this content —
                    # use it as link target (avoids copying entirely)
                    canonical = None
                    # Track which folder the match came from
                    match_folder = mount_rel.split(os.sep)[0] if os.sep in mount_rel else mount_rel.split("/")[0]
                    for e in group:
                        link_map[e.rel] = ("__abs__", full_path)
                        saved_bytes += e.size
                        crossrun_count += 1
                        crossrun_bytes += e.size
                    crossrun_sources[match_folder] += len(group)
                    break

        if canonical is not None:
            # Normal dedup: first file is canonical, rest are linked
            unique_entries.append(canonical)
            for dup in group[1:]:
                link_map[dup.rel] = canonical.rel
                saved_bytes += dup.size

    dup_count = len(link_map)
    within_run = dup_count - crossrun_count
    total_files = len(entries)

    print(f"\r  {C.GREEN}Dedup complete:{C.RESET}                              ")
    print(f"    Unique files:    {C.BOLD}{len(unique_entries)}{C.RESET}")
    if within_run > 0:
        print(f"    Within-run dups: {C.BOLD}{within_run}{C.RESET} files "
              f"(identical files in source)")
    if crossrun_count > 0:
        print(f"    Cross-run dups:  {C.BOLD}{crossrun_count}{C.RESET} files "
              f"({C.GREEN}{fmt_size(crossrun_bytes)}{C.RESET}) — "
              f"already on drive, will link instead of copy")
        for folder, count in sorted(crossrun_sources.items(), key=lambda x: -x[1]):
            print(f"      → {C.CYAN}{folder}/{C.RESET}: {count} files matched")
    print(f"    Total duplicates:{C.BOLD} {dup_count}{C.RESET} "
          f"({fmt_pct(dup_count, total_files)} of files)")
    print(f"    Space saved:     {C.GREEN}{C.BOLD}{fmt_size(saved_bytes)}{C.RESET} "
          f"({fmt_pct(saved_bytes, sum(e.size for e in entries))} reduction)")

    return unique_entries, link_map, saved_bytes


def create_links(link_map, dst_root):
    """
    Create hard links for deduplicated files.
    Falls back to symlinks if hard links fail (e.g. FAT32/exFAT USB).
    Falls back to actual copy as last resort.
    """
    if not link_map:
        return

    print(f"  {C.DIM}Creating {len(link_map)} links for duplicates...{C.RESET}", end="", flush=True)
    hardlink_ok = 0
    symlink_ok = 0
    copy_fallback = 0
    errors = 0

    for dup_rel, target in link_map.items():
        dst_dup = os.path.join(dst_root, dup_rel)
        # Target is either a rel path or ("__abs__", full_path) for cross-run dedup
        if isinstance(target, tuple) and target[0] == "__abs__":
            dst_canonical = target[1]
        else:
            dst_canonical = os.path.join(dst_root, target)

        os.makedirs(os.path.dirname(dst_dup), exist_ok=True)

        # Try hard link first (fastest, no extra space)
        try:
            os.link(dst_canonical, dst_dup)
            hardlink_ok += 1
            continue
        except OSError:
            pass

        # Try symlink (works on most filesystems)
        try:
            # Compute relative path from dup to canonical
            rel_target = os.path.relpath(dst_canonical, os.path.dirname(dst_dup))
            os.symlink(rel_target, dst_dup)
            # Verify the symlink actually resolves (NTFS via Linux creates
            # symlinks that don't work)
            if os.path.isfile(dst_dup):
                symlink_ok += 1
                continue
            else:
                # Broken symlink — remove and fall through to copy
                os.unlink(dst_dup)
        except OSError:
            pass

        # Last resort: actual copy
        try:
            shutil.copy2(dst_canonical, dst_dup)
            copy_fallback += 1
        except OSError as e:
            errors += 1

    results = []
    if hardlink_ok:
        results.append(f"{hardlink_ok} hard links")
    if symlink_ok:
        results.append(f"{symlink_ok} symlinks")
    if copy_fallback:
        results.append(f"{copy_fallback} copies (fallback)")
    if errors:
        results.append(f"{C.RED}{errors} errors{C.RESET}")

    print(f"\r  {C.GREEN}Links created: {', '.join(results)}{C.RESET}                    ")


# ════════════════════════════════════════════════════════════════════════════
# PROGRESS TRACKER
# ════════════════════════════════════════════════════════════════════════════
class Progress:
    def __init__(self, total_bytes, total_files):
        self.total_bytes = total_bytes
        self.total_files = total_files
        self.bytes_done = 0
        self.files_done = 0
        self.lock = threading.Lock()
        self.start = time.time()
        self._last_print = 0

    def update(self, nbytes, nfiles=0):
        with self.lock:
            self.bytes_done += nbytes
            self.files_done += nfiles

    def display(self):
        now = time.time()
        if now - self._last_print < 0.08:
            return
        self._last_print = now
        elapsed = now - self.start
        if elapsed == 0:
            return

        pct = (self.bytes_done / self.total_bytes * 100) if self.total_bytes else 100
        speed = self.bytes_done / elapsed
        eta = (self.total_bytes - self.bytes_done) / speed if speed > 0 else 0

        bar_w = 30
        filled = int(bar_w * min(pct, 100) / 100)
        bar = "█" * filled + "░" * (bar_w - filled)

        sys.stdout.write(
            f"\r  {C.CYAN}{bar}{C.RESET} {pct:5.1f}%  "
            f"{fmt_size(self.bytes_done)}/{fmt_size(self.total_bytes)}  "
            f"{C.GREEN}{fmt_speed(speed)}{C.RESET}  "
            f"{self.files_done}/{self.total_files} files  "
            f"ETA {fmt_time(eta)}   "
        )
        sys.stdout.flush()

    def finish(self):
        elapsed = time.time() - self.start
        speed = self.bytes_done / elapsed if elapsed > 0 else 0
        print(f"\r  {C.GREEN}{'█' * 30}{C.RESET} 100%  "
              f"{fmt_size(self.bytes_done)} in {fmt_time(elapsed)}  "
              f"avg {C.GREEN}{fmt_speed(speed)}{C.RESET}  "
              f"{self.files_done} files                ")


# ════════════════════════════════════════════════════════════════════════════
# COPY ENGINE — TRUE BLOCK-LEVEL WRITES
#
# The problem: USB drives have terrible per-file write latency. Copying
# 3000 small files = 3000 separate open/write/close/flush operations,
# each one hitting the USB controller individually.
#
# The solution: Bundle small files into one big tar archive, write it as
# a single sequential block to USB (one fast write), then extract locally
# on the USB. Large files still copy individually with big buffers.
#
# This is what enterprize backup tools (Dell/EMC, Veeam) actually do —
# they never write thousands of tiny files individually.
# ════════════════════════════════════════════════════════════════════════════

SMALL_FILE_THRESHOLD = 1 * 1024 * 1024  # 1 MB — files below this get bundled

TAR_BUNDLE_NAME = ".fast_copy_bundle.tar"


def split_by_size(entries):
    """Split entries into small files (bundle) and large files (individual copy)."""
    small = [e for e in entries if e.size < SMALL_FILE_THRESHOLD]
    large = [e for e in entries if e.size >= SMALL_FILE_THRESHOLD]
    return small, large


def copy_block_stream(small_entries, dst_root, progress, cancel_check=None):
    """
    TRUE BLOCK-LEVEL WRITE for small files:
      1. Read all small files in physical disk order (sequential source reads)
      2. Stream them into a tar archive written directly to USB as ONE file
      3. Extract tar locally on USB (source = USB itself, fast)
      4. Delete the tar bundle

    Why this is fast:
      - Writing 1 tar file = 1 sequential USB write (no per-file overhead)
      - Reading from source in physical order = sequential disk reads
      - Extraction reads from USB itself (no cross-device latency)
    """
    if not small_entries:
        return

    small_size = sum(e.size for e in small_entries)
    tar_path = os.path.join(dst_root, TAR_BUNDLE_NAME)

    print(f"  {C.CYAN}Bundling {len(small_entries)} small files ({fmt_size(small_size)}) "
          f"into single block stream...{C.RESET}")

    # ── Step 1: Write tar archive to USB (one big sequential write) ───
    os.makedirs(dst_root, exist_ok=True)

    try:
        with tarfile.open(tar_path, "w") as tar:
            for entry in small_entries:
                if cancel_check and cancel_check():
                    break
                try:
                    # Read file into memory (it's small, fits easily)
                    with open(entry.src, "rb") as f:
                        data = f.read()

                    # Create tar entry with metadata
                    info = tarfile.TarInfo(name=entry.rel)
                    info.size = len(data)
                    try:
                        st = os.stat(entry.src)
                        info.mtime = st.st_mtime
                        info.mode = st.st_mode
                    except OSError:
                        info.mtime = time.time()

                    tar.addfile(info, io.BytesIO(data))
                    progress.update(len(data), 1)
                    progress.display()

                except (OSError, IOError) as e:
                    print(f"\n  {C.RED}Error bundling: {entry.rel}: {e}{C.RESET}")
                    progress.update(entry.size, 1)

        tar_size = os.path.getsize(tar_path)
        print(f"\n  {C.GREEN}Block written: {fmt_size(tar_size)} bundle on USB{C.RESET}")

    except (OSError, IOError) as e:
        print(f"\n  {C.RED}Failed to write tar bundle: {e}{C.RESET}")
        # Cleanup and fall back
        if os.path.exists(tar_path):
            os.remove(tar_path)
        print(f"  {C.YELLOW}Falling back to file-by-file copy...{C.RESET}")
        copy_individual(small_entries, dst_root, progress,
                        bytearray(DEFAULT_BUFFER_MB * 1024 * 1024), cancel_check)
        return

    # ── Step 2: Extract tar locally on USB (per-file, skip errors) ──
    print(f"  {C.CYAN}Extracting block to individual files on USB...{C.RESET}", end="", flush=True)

    extracted = 0
    extract_errors = []

    try:
        with tarfile.open(tar_path, "r") as tar:
            for member in tar.getmembers():
                try:
                    # Validate member name to prevent path traversal
                    if member.name.startswith('/') or '..' in member.name.split('/'):
                        extract_errors.append((member.name, "blocked: unsafe path"))
                        continue

                    dst_member = os.path.join(dst_root, member.name)

                    # If file exists with read-only perms (e.g. .git/objects),
                    # make it writable so we can overwrite
                    if os.path.exists(dst_member) and not os.access(dst_member, os.W_OK):
                        try:
                            os.chmod(dst_member, 0o644)
                        except OSError:
                            pass

                    try:
                        tar.extract(member, path=dst_root, filter='data')
                    except TypeError:
                        # Python <3.12 doesn't support filter argument
                        tar.extract(member, path=dst_root)
                    extracted += 1

                except (OSError, tarfile.TarError) as e:
                    extract_errors.append((member.name, str(e)))

        if extract_errors:
            print(f"\r  {C.YELLOW}Extracted {extracted} files, "
                  f"{len(extract_errors)} errors{C.RESET}                    ")
            for name, err in extract_errors[:5]:
                print(f"    {C.YELLOW}→ {name}: {err}{C.RESET}")
            if len(extract_errors) > 5:
                print(f"    ... and {len(extract_errors) - 5} more")
        else:
            print(f"\r  {C.GREEN}Extracted {extracted} files from block{C.RESET}                    ")

    except (OSError, tarfile.TarError) as e:
        print(f"\n  {C.RED}Failed to open tar bundle: {e}{C.RESET}")

    # ── Step 3: Remove tar bundle ─────────────────────────────────────
    try:
        os.remove(tar_path)
    except OSError:
        print(f"  {C.YELLOW}Note: Could not remove bundle {TAR_BUNDLE_NAME} — delete manually{C.RESET}")


def copy_individual(entries, dst_root, progress, buf, cancel_check=None):
    """Copy large files individually with big buffers in physical disk order."""
    mv = memoryview(buf)

    for entry in entries:
        # Check for cancellation between files
        if cancel_check and cancel_check():
            return

        dst_path = os.path.join(dst_root, entry.rel)
        dst_dir = os.path.dirname(dst_path)

        try:
            os.makedirs(dst_dir, exist_ok=True)

            if entry.size == 0:
                open(dst_path, "wb").close()
                progress.update(0, 1)
                progress.display()
                continue

            with open(entry.src, "rb") as fin, open(dst_path, "wb") as fout:
                while True:
                    # Check for cancellation during large file copy
                    if cancel_check and cancel_check():
                        return
                    n = fin.readinto(buf)
                    if not n:
                        break
                    fout.write(mv[:n])
                    progress.update(n)
                    progress.display()

            # Preserve timestamps
            try:
                st = os.stat(entry.src)
                os.utime(dst_path, (st.st_atime, st.st_mtime))
            except OSError:
                pass

            progress.update(0, 1)

        except (OSError, IOError) as e:
            print(f"\n  {C.RED}Error: {entry.rel}: {e}{C.RESET}")
            progress.update(entry.size, 1)


def copy_hybrid(entries, dst_root, progress, buf_size, cancel_check=None):
    """
    Hybrid block copy engine:
      - Small files (<1MB): bundled into tar → single block write → extract
      - Large files (>=1MB): individual copy with large buffers
    """
    small, large = split_by_size(entries)
    small_size = sum(e.size for e in small)
    large_size = sum(e.size for e in large)

    print(f"  Strategy:")
    print(f"    Small files (<1MB): {C.BOLD}{len(small)}{C.RESET} files, "
          f"{C.BOLD}{fmt_size(small_size)}{C.RESET} → block stream")
    print(f"    Large files (≥1MB): {C.BOLD}{len(large)}{C.RESET} files, "
          f"{C.BOLD}{fmt_size(large_size)}{C.RESET} → individual copy")
    print()

    # Copy large files first (they benefit most from physical ordering)
    if large:
        print(f"  {C.BOLD}── Large files ──{C.RESET}")
        buf = bytearray(buf_size)
        copy_individual(large, dst_root, progress, buf, cancel_check)
        if cancel_check and cancel_check():
            return
        print()

    # Block-stream small files
    if small:
        print(f"  {C.BOLD}── Small files (block stream) ──{C.RESET}")
        copy_block_stream(small, dst_root, progress, cancel_check)


# ════════════════════════════════════════════════════════════════════════════
# VERIFICATION
# ════════════════════════════════════════════════════════════════════════════
def verify_copy(entries, link_map, dst_root):
    """Check existence + file size for all files (unique + linked).
    Uses a single os.walk pass instead of per-file stat calls."""
    total_to_check = len(entries) + len(link_map)
    print(f"\n  {C.DIM}Verifying {total_to_check} files...{C.RESET}", end="", flush=True)

    # Build expected files: rel_path → expected_size (None = just check exists)
    expected = {}
    for entry in entries:
        expected[entry.rel] = entry.size
    for dup_rel in link_map:
        expected[dup_rel] = None  # links: just check existence

    # Single walk of destination — much faster than per-file stat on USB
    found = {}
    for root, dirs, files in os.walk(dst_root):
        for fname in files:
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, dst_root)
            if rel in expected:
                try:
                    found[rel] = os.path.getsize(full)
                except OSError:
                    pass

    mismatches = []
    missing = []
    for rel, exp_size in expected.items():
        if rel not in found:
            tag = " (link)" if exp_size is None else ""
            missing.append(f"{rel}{tag}")
        elif exp_size is not None and found[rel] != exp_size:
            mismatches.append((rel, exp_size, found[rel]))

    total_checked = len(expected)

    if not missing and not mismatches:
        print(f"\r  {C.GREEN}✓ Verified: all {total_checked} files OK{C.RESET}               ")
        return True
    else:
        print(f"\r  {C.RED}✗ Verification failed:{C.RESET}")
        for m in missing[:10]:
            print(f"    {C.RED}MISSING: {m}{C.RESET}")
        for rel, exp, act in mismatches[:10]:
            print(f"    {C.RED}SIZE MISMATCH: {rel} ({exp} → {act}){C.RESET}")
        shown = min(len(missing), 10) + min(len(mismatches), 10)
        remain = len(missing) + len(mismatches) - shown
        if remain > 0:
            print(f"    ... and {remain} more")
        return False


# ════════════════════════════════════════════════════════════════════════════
# SKIP IDENTICAL FILES (incremental mode)
# ════════════════════════════════════════════════════════════════════════════
def filter_unchanged(entries, link_map, dst_root, threads=DEFAULT_THREADS):
    """
    Compare source files against existing destination files.
    Skip files that already exist at destination with identical content.

    Strategy (fast to slow):
      1. If destination file doesn't exist → must copy
      2. If sizes differ → must copy
      3. If sizes match → hash both and compare → skip if identical

    Returns (to_copy, to_link, skipped_count, skipped_bytes)
    """
    print(f"  {C.DIM}Checking destination for existing files...{C.RESET}", end="", flush=True)

    need_copy = []     # entries that need copying
    need_hash = []     # entries where dest exists with same size → need hash check
    skipped = []       # entries skipped (identical)
    skipped_bytes = 0

    # ── Quick pass: size check ────────────────────────────────────────
    for entry in entries:
        dst_path = os.path.join(dst_root, entry.rel)

        if not os.path.exists(dst_path):
            need_copy.append(entry)
            continue

        try:
            dst_size = os.path.getsize(dst_path)
        except OSError:
            need_copy.append(entry)
            continue

        if dst_size != entry.size:
            # Different size → must overwrite
            need_copy.append(entry)
        else:
            # Same size → need hash comparison
            need_hash.append(entry)

    print(f"\r  {C.DIM}Quick check: {len(need_copy)} new/changed, "
          f"{len(need_hash)} same-size need hash check{C.RESET}          ")

    if not need_hash:
        # Nothing to hash-check, everything is new
        return need_copy, link_map, 0, 0

    # ── Hash pass: compare content of same-size files ─────────────────
    print(f"  {C.DIM}Hashing {len(need_hash)} files to check for changes...{C.RESET}", end="", flush=True)

    src_hashes = [None] * len(need_hash)
    dst_hashes = [None] * len(need_hash)

    def hash_pair(idx):
        entry = need_hash[idx]
        dst_path = os.path.join(dst_root, entry.rel)
        src_hashes[idx] = hash_file(entry.src)
        dst_hashes[idx] = hash_file(dst_path)

    done_count = [0]
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(hash_pair, i) for i in range(len(need_hash))]
        for f in as_completed(futures):
            f.result()
            with lock:
                done_count[0] += 1
                if done_count[0] % 200 == 0:
                    print(f"\r  {C.DIM}Hashing... {done_count[0]}/{len(need_hash)}{C.RESET}",
                          end="", flush=True)

    # Compare hashes
    for i, entry in enumerate(need_hash):
        if (src_hashes[i] is not None and dst_hashes[i] is not None
                and src_hashes[i] == dst_hashes[i]):
            skipped.append(entry)
            skipped_bytes += entry.size
        else:
            need_copy.append(entry)

    # Also filter link_map — skip links where destination already exists
    new_link_map = {}
    skipped_links = 0
    for dup_rel, canonical_rel in link_map.items():
        dst_path = os.path.join(dst_root, dup_rel)
        if os.path.exists(dst_path):
            skipped_links += 1
        else:
            new_link_map[dup_rel] = canonical_rel

    print(f"\r  {C.GREEN}Incremental check complete:{C.RESET}                              ")
    print(f"    To copy:   {C.BOLD}{len(need_copy)}{C.RESET} files "
          f"({fmt_size(sum(e.size for e in need_copy))})")
    print(f"    Skipped:   {C.BOLD}{len(skipped)}{C.RESET} files unchanged "
          f"({C.GREEN}{fmt_size(skipped_bytes)}{C.RESET})")
    if skipped_links:
        print(f"    Links:     {C.BOLD}{skipped_links}{C.RESET} already exist, "
              f"{C.BOLD}{len(new_link_map)}{C.RESET} to create")

    return need_copy, new_link_map, len(skipped) + skipped_links, skipped_bytes


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Block-order fast copy with dedup — reads files in physical "
                    "disk order, deduplicates identical files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("source", help="Source folder, file, or glob pattern (e.g. *.zip)")
    parser.add_argument("destination", help="Destination (USB drive path, etc)")
    parser.add_argument("--buffer", type=int, default=DEFAULT_BUFFER_MB,
                        help=f"Buffer size in MB (default: {DEFAULT_BUFFER_MB})")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                        help=f"Threads for hashing/layout (default: {DEFAULT_THREADS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show copy plan without copying")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip post-copy verification")
    parser.add_argument("--no-dedup", action="store_true",
                        help="Disable deduplication")
    parser.add_argument("--force", action="store_true",
                        help="Skip space check, copy even if not enough space")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite all files, skip identical-file detection")
    parser.add_argument("--exclude", action="append", default=[],
                        help="Exclude files/dirs by name (can use multiple times)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Disable persistent hash cache (cross-run dedup database)")
    args = parser.parse_args()

    src_arg = args.source
    dst = os.path.abspath(args.destination)
    buf_size = args.buffer * 1024 * 1024

    # ── Resolve source: directory, single file, or glob pattern ──────
    src_mode = None  # "dir", "file", or "glob"
    glob_files = []

    src = os.path.abspath(src_arg)
    if os.path.isdir(src):
        src_mode = "dir"
    elif os.path.isfile(src):
        src_mode = "file"
    else:
        # Try glob expansion (handles wildcards like *.zip)
        glob_files = sorted(globmod.glob(src_arg))
        if not glob_files:
            # Also try with abspath
            glob_files = sorted(globmod.glob(src))
        # Filter to files only
        glob_files = [f for f in glob_files if os.path.isfile(f)]
        if glob_files:
            src_mode = "glob"
        else:
            print(f"{C.RED}Error: Source '{src_arg}' — no matching files or directory found{C.RESET}")
            sys.exit(1)

    if src_mode == "glob":
        src_display = src_arg
        # Use common parent as the "source root" for relative paths
        src = os.path.commonpath([os.path.abspath(f) for f in glob_files])
        if os.path.isfile(src):
            src = os.path.dirname(src)
    elif src_mode == "file":
        src_display = src
    else:
        src_display = src

    banner("FAST BLOCK-ORDER COPY")
    print(f"  Source:      {C.BOLD}{src_display}{C.RESET}")
    if src_mode == "glob":
        print(f"               {C.DIM}{len(glob_files)} files matched{C.RESET}")
    elif src_mode == "file":
        print(f"               {C.DIM}(single file){C.RESET}")
    print(f"  Destination: {C.BOLD}{dst}{C.RESET}")
    print(f"  Buffer:      {args.buffer} MB")
    print(f"  Dedup:       {'disabled' if args.no_dedup else 'enabled'}")
    print(f"  Hash cache:  {'disabled' if args.no_cache else 'enabled'}")
    print(f"  Overwrite:   {'always' if args.overwrite else 'skip identical'}")
    print(f"  Platform:    {_system}")
    print()

    # ── Phase 1: Scan ─────────────────────────────────────────────────
    banner("Phase 1 — Scanning source")

    if src_mode == "file":
        # Single file — build entry directly
        fname = os.path.basename(src)
        sz = os.path.getsize(src)
        entries = [FileEntry(src=src, rel=fname, size=sz,
                             physical_offset=0, content_hash=None)]
        errors = []
        print(f"  {C.GREEN}Found 1 file{C.RESET} ({fmt_size(sz)})")
        src = os.path.dirname(src)  # parent becomes "source root" for dst layout
    elif src_mode == "glob":
        # Glob — build entries from matched files
        entries = []
        errors = []
        for fpath in glob_files:
            abs_f = os.path.abspath(fpath)
            rel = os.path.relpath(abs_f, src)
            try:
                sz = os.path.getsize(abs_f)
                entries.append(FileEntry(src=abs_f, rel=rel, size=sz,
                                         physical_offset=0, content_hash=None))
            except OSError as e:
                errors.append((abs_f, str(e)))
        print(f"  {C.GREEN}Found {len(entries)} files{C.RESET}")
    else:
        entries, errors = scan_source(src, dst, args.exclude)

    if not entries:
        print(f"  {C.YELLOW}No files found.{C.RESET}")
        sys.exit(0)

    total_size = sum(e.size for e in entries)
    total_files = len(entries)
    avg_size = total_size / total_files if total_files else 0
    print(f"  Total: {C.BOLD}{fmt_size(total_size)}{C.RESET} in "
          f"{C.BOLD}{total_files}{C.RESET} files  "
          f"(avg {fmt_size(avg_size)}/file)")

    # ── Open dedup database ──────────────────────────────────────────
    dedup_db = None
    if not args.no_dedup and not args.no_cache:
        os.makedirs(dst, exist_ok=True)
        try:
            dedup_db = DedupDB(dst)
        except Exception as e:
            print(f"  {C.YELLOW}Warning: could not open hash cache: {e}{C.RESET}")

    # ── Phase 2: Deduplication ────────────────────────────────────────
    link_map = {}
    saved_bytes = 0
    copy_entries = entries

    if not args.no_dedup:
        banner("Phase 2 — Deduplication")
        copy_entries, link_map, saved_bytes = deduplicate(entries, args.threads, dedup_db)

    unique_size = sum(e.size for e in copy_entries)

    # ── Phase 2b: Skip unchanged files ────────────────────────────────
    skipped_count = 0
    skipped_bytes = 0

    if not args.overwrite and os.path.isdir(dst):
        banner("Phase 2b — Incremental check")
        copy_entries, link_map, skipped_count, skipped_bytes = filter_unchanged(
            copy_entries, link_map, dst, args.threads
        )
        unique_size = sum(e.size for e in copy_entries)

        if not copy_entries and not link_map:
            if dedup_db:
                dedup_db.close()
            banner("DONE — Nothing to copy")
            print(f"  All {skipped_count} files are already up to date.")
            print()
            sys.exit(0)

    # ── Phase 3: Space check ──────────────────────────────────────────
    banner("Phase 3 — Space check")
    required = unique_size  # after dedup, only unique data needs disk space
    print(f"  Data to write: {C.BOLD}{fmt_size(required)}{C.RESET}"
          + (f" (after dedup saved {fmt_size(saved_bytes)})" if saved_bytes > 0 else ""))

    if not check_destination_space(dst, required, args.force):
        sys.exit(1)

    # ── Phase 4: Resolve physical layout ──────────────────────────────
    banner("Phase 4 — Mapping physical disk layout")
    copy_entries = resolve_physical_offsets(copy_entries, args.threads)

    if args.dry_run:
        small, large = split_by_size(copy_entries)
        small_sz = sum(e.size for e in small)
        large_sz = sum(e.size for e in large)
        print(f"\n  {C.YELLOW}DRY RUN — Copy strategy:{C.RESET}\n")
        print(f"    Small files (<1MB): {C.BOLD}{len(small)}{C.RESET} files, "
              f"{C.BOLD}{fmt_size(small_sz)}{C.RESET} → single block stream (tar)")
        print(f"    Large files (≥1MB): {C.BOLD}{len(large)}{C.RESET} files, "
              f"{C.BOLD}{fmt_size(large_sz)}{C.RESET} → individual copy")
        print(f"\n  {C.YELLOW}First 20 files in disk order:{C.RESET}\n")
        for i, e in enumerate(copy_entries[:20]):
            tag = "BLK" if e.size < SMALL_FILE_THRESHOLD else "IND"
            print(f"  {i+1:4d}. [{tag}] offset={e.physical_offset:>14d}  "
                  f"size={fmt_size(e.size):>10s}  {e.rel}")
        if len(copy_entries) > 20:
            print(f"  ... and {len(copy_entries) - 20} more files")
        if link_map:
            print(f"\n  Plus {len(link_map)} duplicate files to be linked")
        print(f"\n  Unique data: {fmt_size(unique_size)}")
        if dedup_db:
            dedup_db.close()
        sys.exit(0)

    # ── Phase 5: Block copy ─────────────────────────────────────────
    banner("Phase 5 — Block copy")
    os.makedirs(dst, exist_ok=True)

    progress = Progress(unique_size, len(copy_entries))
    t0 = time.time()
    copy_hybrid(copy_entries, dst, progress, buf_size)
    progress.finish()

    # Create links for duplicates
    if link_map:
        create_links(link_map, dst)

    elapsed = time.time() - t0
    speed = unique_size / elapsed if elapsed > 0 else 0

    # ── Update dedup database with copied files ──────────────────────
    if dedup_db:
        dst_rows = []
        for e in copy_entries:
            if not e.content_hash:
                continue  # skip unhashed files — will be cached on next run
            dst_rows.append((e.rel, e.size, e.content_hash))
        if dst_rows:
            dedup_db.store_dest_batch(dst_rows)
        dedup_db.close()

    # ── Verify ────────────────────────────────────────────────────────
    if not args.no_verify:
        verify_copy(copy_entries, link_map, dst)

    # ── Summary ───────────────────────────────────────────────────────
    banner("DONE")
    print(f"  Files:   {C.BOLD}{total_files}{C.RESET} total"
          + (f" ({len(copy_entries)} copied + {len(link_map)} linked)" if link_map else ""))
    if skipped_count:
        print(f"  Skipped: {C.BOLD}{skipped_count}{C.RESET} unchanged files "
              f"({C.GREEN}{fmt_size(skipped_bytes)}{C.RESET})")
    print(f"  Data:    {C.BOLD}{fmt_size(unique_size)}{C.RESET} written"
          + (f" ({fmt_size(saved_bytes)} saved by dedup)" if saved_bytes > 0 else ""))
    print(f"  Time:    {C.BOLD}{fmt_time(elapsed)}{C.RESET}")
    print(f"  Speed:   {C.GREEN}{C.BOLD}{fmt_speed(speed)}{C.RESET}")
    print()


if __name__ == "__main__":
    main()
