#!/usr/bin/env python3
"""
Build script — compiles fast_copy into standalone executables.

Usage:
  python build.py              # build both CLI and GUI
  python build.py cli          # build CLI only
  python build.py gui          # build GUI only
  python build.py --clean      # clean build artifacts first

Output:
  dist/fast_copy       — CLI executable
  dist/fast_copy_gui   — GUI executable (opens in browser)
"""

import os
import sys
import shutil
import platform
import subprocess


def install_deps():
    """Install build dependencies."""
    deps = {
        "pyinstaller": "PyInstaller",
        "xxhash": "xxhash",
    }
    for pip_name, import_name in deps.items():
        try:
            __import__(import_name)
            print(f"  ✓ {pip_name}")
        except ImportError:
            print(f"  Installing {pip_name}...")
            try:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", pip_name, "--quiet",
                    "--disable-pip-version-check",
                ])
            except subprocess.CalledProcessError:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", pip_name, "--quiet",
                    "--disable-pip-version-check", "--break-system-packages",
                ])


def build_target(name, script, console=True):
    """Build a single target with PyInstaller."""
    ext = ".exe" if platform.system() == "Windows" else ""
    out = f"{name}{ext}"

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--name", name,
        "--clean",
        "--noupx",
        "--console" if console else "--windowed",
        "--hidden-import=xxhash",
        script,
    ]

    print(f"\n  Building {out}...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        binary = os.path.join("dist", out)
        size_mb = os.path.getsize(binary) / (1024 * 1024)
        print(f"  ✓ {binary} ({size_mb:.1f} MB)")
        return True
    else:
        print(f"  ✗ Failed to build {name}")
        if result.stderr:
            for line in result.stderr.strip().split('\n')[-5:]:
                print(f"    {line}")
        return False


def main():
    clean = "--clean" in sys.argv
    targets_arg = [a for a in sys.argv[1:] if not a.startswith("--")]
    targets = targets_arg if targets_arg else ["cli", "gui"]

    print(f"Fast Copy Builder — {platform.system()} ({platform.machine()})")
    print(f"{'─' * 50}")

    # Check source files exist
    for script in ["fast_copy.py", "fast_copy_gui.py"]:
        if not os.path.exists(script):
            needed = "cli" if "fast_copy.py" == script else "gui"
            if needed in targets:
                print(f"  Error: {script} not found in current directory")
                sys.exit(1)

    # Install deps
    print("\nDependencies:")
    install_deps()

    # Clean
    if clean:
        for d in ("build", "dist", "__pycache__"):
            if os.path.exists(d):
                shutil.rmtree(d)
        for f in os.listdir("."):
            if f.endswith(".spec"):
                os.remove(f)
        print("\nCleaned build artifacts.")

    # Build
    print(f"\nBuilding targets: {', '.join(targets)}")

    results = {}
    if "cli" in targets:
        results["cli"] = build_target("fast_copy", "fast_copy.py", console=True)
    if "gui" in targets:
        results["gui"] = build_target("fast_copy_gui", "fast_copy_gui.py", console=True)

    # Summary
    ext = ".exe" if platform.system() == "Windows" else ""
    print(f"\n{'─' * 50}")
    print("Build complete:\n")

    if results.get("cli"):
        print(f"  CLI:  dist/fast_copy{ext}")
        print(f'        fast_copy "C:\\Source" "E:\\Dest"')
        print(f'        fast_copy /source /dest')
    if results.get("gui"):
        print(f"\n  GUI:  dist/fast_copy_gui{ext}")
        print(f"        Double-click or run from terminal — opens in browser")
    print()


if __name__ == "__main__":
    main()
