#!/usr/bin/env python3
"""
Build script — compiles fast_copy.py into a standalone executable.

Run on each target OS:
  python build.py           # builds for current OS
  python build.py --clean   # clean build artifacts first

Requirements:
  pip install pyinstaller
"""

import os
import sys
import shutil
import platform
import subprocess

def main():
    clean = "--clean" in sys.argv

    print(f"Building fast_copy for {platform.system()} ({platform.machine()})...")

    # Install dependencies
    deps = {
        "pyinstaller": "PyInstaller",
        "xxhash": "xxhash",
    }
    for pip_name, import_name in deps.items():
        try:
            __import__(import_name)
            print(f"  ✓ {pip_name} found")
        except ImportError:
            print(f"  Installing {pip_name}...")
            try:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", pip_name, "--quiet",
                    "--disable-pip-version-check",
                ])
            except subprocess.CalledProcessError:
                # Some systems (e.g. Debian/Ubuntu) need this flag
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", pip_name, "--quiet",
                    "--disable-pip-version-check", "--break-system-packages",
                ])

    if clean:
        for d in ("build", "dist", "__pycache__"):
            if os.path.exists(d):
                shutil.rmtree(d)
        for f in os.listdir("."):
            if f.endswith(".spec"):
                os.remove(f)
        print("Cleaned build artifacts.")

    # Build
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--name", "fast_copy",
        "--clean",
        "--noupx",                    # UPX can cause false antivirus flags
        "--console",
        "--hidden-import=xxhash",     # bundle xxhash for 10x faster dedup
        "fast_copy.py",
    ]

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    if result.returncode == 0:
        ext = ".exe" if platform.system() == "Windows" else ""
        binary = os.path.join("dist", f"fast_copy{ext}")
        size_mb = os.path.getsize(binary) / (1024 * 1024)
        print(f"\n✓ Built successfully: {binary} ({size_mb:.1f} MB)")
        print(f"\nUsage:")
        print(f'  {binary} "C:\\Source\\Folder" "E:\\USB\\Destination"')
        print(f'  {binary} /home/user/data /media/usb/data')
    else:
        print("\n✗ Build failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
