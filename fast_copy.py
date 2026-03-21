#!/usr/bin/env python3
"""
FAST BLOCK-ORDER COPY — Copies folders at maximum sequential disk speed.

Features:
  • Reads files in PHYSICAL disk order (eliminates random seeks)
  • Pre-flight space check (compares source size vs USB free space)
  • Content-aware deduplication (hashes files, copies each unique file once,
    hard-links duplicates — like Dell's backup dedup)
  • Large I/O buffers (64MB default)
  • Post-copy verification

Usage:
  python fast_copy.py <source_folder> <usb_destination>

Examples:
  python fast_copy.py "C:\\Projects" "E:\\Backup\\Projects"
  python fast_copy.py /home/user/data /media/usb/data
  python fast_copy.py /data /mnt/usb --no-dedup          # skip dedup
  python fast_copy.py /data /mnt/usb --force              # skip space check

Options:
  --buffer MB     Read/write buffer size in MB (default: 64)
  --threads N     Threads for hashing & layout resolution (default: 4)
  --dry-run       Show copy plan without copying
  --no-verify     Skip post-copy verification
  --no-dedup      Disable deduplication (copy all files even if identical)
  --force         Skip space check and copy anyway

Build standalone executable:
  pip install pyinstaller
  pyinstaller --onefile --name fast_copy fast_copy.py
"""

import os
import sys
import stat
import time
import struct
import ctypes
import shutil
import hashlib
import tarfile
import io
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
HASH_CHUNK = 1 * 1024 * 1024   # 1MB chunks for hashing
HASH_ALGO = "xxh64"             # try xxhash first, fallback to md5

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
# HASHING — use xxhash if available (10x faster), fallback to md5
# ════════════════════════════════════════════════════════════════════════════
try:
    import xxhash
    def new_hasher():
        return xxhash.xxh64()
    _hash_name = "xxh64"
except ImportError:
    def new_hasher():
        return hashlib.md5()
    _hash_name = "md5"


def hash_file(filepath, buf_size=HASH_CHUNK):
    """Hash file contents. Returns hex digest string."""
    h = new_hasher()
    try:
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(buf_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


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
    if _system == "Windows":
        return get_physical_offset_windows(filepath)
    elif _system == "Linux":
        return get_physical_offset_linux(filepath)
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
    exclude_names = {TAR_BUNDLE_NAME}  # always skip our own tar bundle
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
def deduplicate(entries, threads=DEFAULT_THREADS):
    """
    Content-aware deduplication (like Dell's backup):
      1. Group files by size (files of different sizes can't be duplicates)
      2. For same-size groups, hash file contents
      3. Return (unique_entries, link_map) where link_map maps
         duplicate rel paths → the rel path of the canonical copy

    This means we only COPY each unique file once. Duplicates become
    hard links on the destination, saving both time and space.
    """
    print(f"  Using hash: {C.BOLD}{_hash_name}{C.RESET}")

    # ── Step 1: Group by size (fast pre-filter) ───────────────────────
    size_groups = defaultdict(list)
    for e in entries:
        size_groups[e.size].append(e)

    # Files with unique size: no possible duplicates
    unique_entries = []
    candidates = []  # same-size groups that need hashing

    for sz, group in size_groups.items():
        if len(group) == 1:
            unique_entries.append(group[0])
        else:
            candidates.extend(group)

    if not candidates:
        print(f"  {C.GREEN}No potential duplicates (all files have unique sizes){C.RESET}")
        return entries, {}, 0

    print(f"  {len(candidates)} files in same-size groups need hashing...")

    # ── Step 2: Hash candidates in parallel ───────────────────────────
    hashes = [None] * len(candidates)

    def do_hash(idx):
        hashes[idx] = hash_file(candidates[idx].src)

    done_count = [0]
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(do_hash, i) for i in range(len(candidates))]
        for f in as_completed(futures):
            f.result()
            with lock:
                done_count[0] += 1
                if done_count[0] % 200 == 0:
                    print(f"\r  {C.DIM}Hashing... {done_count[0]}/{len(candidates)}{C.RESET}",
                          end="", flush=True)

    # Update entries with hashes
    hashed_entries = [
        FileEntry(e.src, e.rel, e.size, e.physical_offset, hashes[i])
        for i, e in enumerate(candidates)
    ]

    # ── Step 3: Group by (size, hash) to find true duplicates ─────────
    hash_groups = defaultdict(list)
    for e in hashed_entries:
        if e.content_hash is not None:
            key = (e.size, e.content_hash)
            hash_groups[key].append(e)
        else:
            # Couldn't hash → treat as unique
            unique_entries.append(e)

    link_map = {}       # duplicate_rel → canonical_rel
    saved_bytes = 0

    for key, group in hash_groups.items():
        # First file is canonical (will be copied), rest are duplicates (will be linked)
        canonical = group[0]
        unique_entries.append(canonical)
        for dup in group[1:]:
            link_map[dup.rel] = canonical.rel
            saved_bytes += dup.size

    dup_count = len(link_map)
    total_files = len(entries)

    print(f"\r  {C.GREEN}Dedup complete:{C.RESET}                              ")
    print(f"    Unique files:    {C.BOLD}{len(unique_entries)}{C.RESET}")
    print(f"    Duplicates:      {C.BOLD}{dup_count}{C.RESET} "
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

    for dup_rel, canonical_rel in link_map.items():
        dst_dup = os.path.join(dst_root, dup_rel)
        dst_canonical = os.path.join(dst_root, canonical_rel)

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
            symlink_ok += 1
            continue
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
# This is what enterprise backup tools (Dell/EMC, Veeam) actually do —
# they never write thousands of tiny files individually.
# ════════════════════════════════════════════════════════════════════════════

SMALL_FILE_THRESHOLD = 1 * 1024 * 1024  # 1 MB — files below this get bundled

TAR_BUNDLE_NAME = ".fast_copy_bundle.tar"


def split_by_size(entries):
    """Split entries into small files (bundle) and large files (individual copy)."""
    small = [e for e in entries if e.size < SMALL_FILE_THRESHOLD]
    large = [e for e in entries if e.size >= SMALL_FILE_THRESHOLD]
    return small, large


def copy_block_stream(small_entries, dst_root, progress):
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
                        bytearray(DEFAULT_BUFFER_MB * 1024 * 1024))
        return

    # ── Step 2: Extract tar locally on USB (per-file, skip errors) ──
    print(f"  {C.CYAN}Extracting block to individual files on USB...{C.RESET}", end="", flush=True)

    extracted = 0
    extract_errors = []

    try:
        with tarfile.open(tar_path, "r") as tar:
            for member in tar.getmembers():
                try:
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


def copy_individual(entries, dst_root, progress, buf):
    """Copy large files individually with big buffers in physical disk order."""
    mv = memoryview(buf)

    for entry in entries:
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


def copy_hybrid(entries, dst_root, progress, buf_size):
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
        copy_individual(large, dst_root, progress, buf)
        print()

    # Block-stream small files
    if small:
        print(f"  {C.BOLD}── Small files (block stream) ──{C.RESET}")
        copy_block_stream(small, dst_root, progress)


# ════════════════════════════════════════════════════════════════════════════
# VERIFICATION
# ════════════════════════════════════════════════════════════════════════════
def verify_copy(entries, link_map, dst_root):
    """Check existence + file size for all files (unique + linked)."""
    print(f"\n  {C.DIM}Verifying...{C.RESET}", end="", flush=True)
    mismatches = []
    missing = []

    # Verify unique files
    for entry in entries:
        dst_path = os.path.join(dst_root, entry.rel)
        if not os.path.exists(dst_path):
            missing.append(entry.rel)
            continue
        dst_size = os.path.getsize(dst_path)
        if dst_size != entry.size:
            mismatches.append((entry.rel, entry.size, dst_size))

    # Verify linked duplicates exist
    for dup_rel in link_map:
        dst_path = os.path.join(dst_root, dup_rel)
        if not os.path.exists(dst_path):
            missing.append(f"{dup_rel} (link)")

    total_checked = len(entries) + len(link_map)

    if not missing and not mismatches:
        print(f"\r  {C.GREEN}✓ Verified: all {total_checked} files OK{C.RESET}               ")
        return True
    else:
        print(f"\r  {C.RED}✗ Verification failed:{C.RESET}")
        for m in missing[:10]:
            print(f"    {C.RED}MISSING: {m}{C.RESET}")
        for rel, exp, act in mismatches[:10]:
            print(f"    {C.RED}SIZE MISMATCH: {rel} ({exp} → {act}){C.RESET}")
        remain = len(missing) + len(mismatches) - 10
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
    parser.add_argument("source", help="Source folder to copy")
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
    args = parser.parse_args()

    src = os.path.abspath(args.source)
    dst = os.path.abspath(args.destination)
    buf_size = args.buffer * 1024 * 1024

    if not os.path.isdir(src):
        print(f"{C.RED}Error: Source '{src}' is not a directory{C.RESET}")
        sys.exit(1)

    banner("FAST BLOCK-ORDER COPY")
    print(f"  Source:      {C.BOLD}{src}{C.RESET}")
    print(f"  Destination: {C.BOLD}{dst}{C.RESET}")
    print(f"  Buffer:      {args.buffer} MB")
    print(f"  Dedup:       {'disabled' if args.no_dedup else 'enabled'}")
    print(f"  Overwrite:   {'always' if args.overwrite else 'skip identical'}")
    print(f"  Platform:    {_system}")
    print()

    # ── Phase 1: Scan ─────────────────────────────────────────────────
    banner("Phase 1 — Scanning source")
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

    # ── Phase 2: Deduplication ────────────────────────────────────────
    link_map = {}
    saved_bytes = 0
    copy_entries = entries

    if not args.no_dedup:
        banner("Phase 2 — Deduplication")
        copy_entries, link_map, saved_bytes = deduplicate(entries, args.threads)

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

