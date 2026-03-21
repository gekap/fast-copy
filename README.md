# fast-copy

High-speed file copier that reads in physical disk order, deduplicates via content hashing, bundles small files into a single block stream, and hard-links duplicates — designed to max out sequential throughput on USB and external drives.

Works on **Linux**, **macOS**, and **Windows**.

## How it works

**fast-copy** copies folders in 5 phases:

1. **Scan** — Walks the source tree and indexes every file with its size.
2. **Dedup** — Groups files by size, then hashes (xxHash or MD5) to find identical content. Each unique file is copied once; duplicates become hard links at the destination.
3. **Space check** — Compares the deduplicated data size against free space on the destination disk before writing anything.
4. **Physical layout mapping** — Resolves the on-disk physical offset of each file (via `FIEMAP`/`ioctl` on Linux, `fcntl` on macOS, `FSCTL` on Windows) and sorts files by physical block order to eliminate random seeks.
5. **Block copy** — Large files (>=1 MB) are copied individually with 64 MB buffers. Small files are bundled into a single tar-like block stream, written sequentially, then extracted — turning thousands of random writes into one continuous write. Duplicates are recreated as hard links.

After copying, all files are verified against their source hashes.

## Installation

```bash
# Run directly with Python (3.8+)
python fast_copy.py <source> <destination>

# Or build a standalone executable
pip install pyinstaller
python build.py
./dist/fast_copy <source> <destination>
```

## Usage

```
usage: fast_copy.py [-h] [--buffer BUFFER] [--threads THREADS] [--dry-run]
                    [--no-verify] [--no-dedup] [--force] [--overwrite]
                    [--exclude EXCLUDE]
                    source destination

positional arguments:
  source             Source folder to copy
  destination        Destination (USB drive path, etc)

options:
  -h, --help         show this help message and exit
  --buffer BUFFER    Buffer size in MB (default: 64)
  --threads THREADS  Threads for hashing/layout (default: 4)
  --dry-run          Show copy plan without copying
  --no-verify        Skip post-copy verification
  --no-dedup         Disable deduplication
  --force            Skip space check, copy even if not enough space
  --overwrite        Overwrite all files, skip identical-file detection
  --exclude EXCLUDE  Exclude files/dirs by name (can use multiple times)
```

## Examples

### Copy a project folder to a USB drive

```bash
# Linux / macOS
python fast_copy.py /home/kai/my-app /mnt/usb/my-app

# Windows
python fast_copy.py "C:\Projects\my-app" "E:\Backup\my-app"
```

### Dry run (preview without copying)

```bash
python fast_copy.py /data /mnt/usb/data --dry-run
```

### Skip deduplication

```bash
python fast_copy.py /data /mnt/usb/data --no-dedup
```

### Exclude directories

```bash
python fast_copy.py /home/user/project /mnt/usb/project --exclude node_modules --exclude .git
```

## Example output

```
$ time python fast_copy.py /home/kai/my-app /mnt/folders/my-app/

────────────────────────────────────────────────────────────
  FAST BLOCK-ORDER COPY
────────────────────────────────────────────────────────────

  Source:      /home/kai/my-app
  Destination: /mnt/folders/my-app
  Buffer:      64 MB
  Dedup:       enabled
  Platform:    Linux


────────────────────────────────────────────────────────────
  Phase 1 — Scanning source
────────────────────────────────────────────────────────────

  Found 59925 files
  Total: 593.2 MB in 59925 files  (avg 10.1 KB/file)

────────────────────────────────────────────────────────────
  Phase 2 — Deduplication
────────────────────────────────────────────────────────────

  Using hash: md5
  55327 files in same-size groups need hashing...
  Dedup complete:
    Unique files:    44454
    Duplicates:      15471 (25.8% of files)
    Space saved:     92.5 MB (15.6% reduction)

────────────────────────────────────────────────────────────
  Phase 3 — Space check
────────────────────────────────────────────────────────────

  Data to write: 500.7 MB (after dedup saved 92.5 MB)
  Destination disk:
    Total:     931.1 GB
    Free:      913.2 GB (98.1% free)
    Required:  500.7 MB
    Headroom:  912.7 GB

  ✓ Enough space

────────────────────────────────────────────────────────────
  Phase 4 — Mapping physical disk layout
────────────────────────────────────────────────────────────

  Disk layout resolved: 44453/44454 files mapped

────────────────────────────────────────────────────────────
  Phase 5 — Block copy
────────────────────────────────────────────────────────────

  Strategy:
    Small files (<1MB): 44410 files, 230.4 MB → block stream
    Large files (≥1MB): 44 files, 270.2 MB → individual copy

  ── Large files ──
  ███████████████░░░░░░░░░░░░░░░  51.3%  256.8 MB/500.7 MB  793.5 MB/s

  ── Small files (block stream) ──
  Bundling 44410 small files (230.4 MB) into single block stream...
  █████████████████████████████░ 100.0%  500.4 MB/500.7 MB  109.5 MB/s
  Block written: 306.6 MB bundle on USB
  Extracted 44410 files from block
  ██████████████████████████████ 100%  500.7 MB in 11.8s  avg 42.5 MB/s
  Links created: 15471 hard links

  ✓ Verified: all 59925 files OK

────────────────────────────────────────────────────────────
  DONE
────────────────────────────────────────────────────────────

  Files:   59925 total (44454 unique + 15471 linked)
  Data:    500.7 MB written (92.5 MB saved by dedup)
  Time:    12.1s
  Speed:   41.2 MB/s


real    0m17.661s
user    0m10.713s
sys     0m9.092s
```

## Key features

- **Block-order reads** — Files are read in physical disk order, eliminating random seeks on HDDs and improving throughput on SSDs.
- **Content deduplication** — Identical files are detected by hash (xxHash when available, MD5 fallback). Each unique file is written once; duplicates become hard links, saving space and write time.
- **Small-file block streaming** — Thousands of small files are bundled into a single sequential write, then extracted — avoids the overhead of creating files one by one.
- **Pre-flight space check** — Verifies the destination has enough free space before writing, accounting for dedup savings.
- **Post-copy verification** — Every copied file is re-hashed and compared against the source to guarantee integrity.
- **64 MB I/O buffers** — Large buffers keep the disk busy and reduce syscall overhead.
- **Cross-platform** — Works on Linux, macOS, and Windows with platform-specific optimizations for physical layout detection.
- **Standalone binary** — Build with PyInstaller for a single-file executable with no Python dependency.

## Support

If you find this tool useful, consider a donation:

| Currency | Address |
|----------|---------|
| **USDC** (ERC-20) | `0xca8a1223300ab7fff6de983d642b96084305cccb` |
| **ETH** (ERC-20) | `0xca8a1223300ab7fff6de983d642b96084305cccb` |
