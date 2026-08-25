#!/usr/bin/env python3

"""
GSR REPOSITORY + HISTORICAL DATA DIAGNOSTIC
--------------------------------------------

READ-ONLY diagnostic.

This script:
- inspects repository structure
- checks Git status/history
- finds historical/data files
- inspects GSR files
- searches Python code for dataset/data-source references
- inspects GSR replay artifacts
- checks common data directories
- reports likely real-data entry points

It does NOT:
- modify existing source files
- delete anything
- git add/commit/push
- contact broker APIs
- expose environment variables/secrets
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from datetime import datetime


ROOT = Path.cwd().resolve()

# Directories that should never be recursively scanned.
SKIP_DIRS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
}

DATA_EXTENSIONS = {
    ".csv",
    ".json",
    ".jsonl",
    ".ndjson",
    ".parquet",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".feather",
    ".pkl",
    ".pickle",
    ".h5",
    ".hdf5",
    ".zip",
    ".gz",
}

CODE_EXTENSIONS = {
    ".py",
    ".toml",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".md",
    ".txt",
}

SEARCH_TERMS = [
    "nifty_3min_dataset",
    "historical",
    "dataset_path",
    "data_path",
    "source_path",
    "csv",
    "jsonl",
    "ndjson",
    "parquet",
    "sqlite",
    "sqlite3",
    "historical_replay",
    "MarketSnapshot",
    "replay_market",
    "load(",
]


def section(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def run_cmd(args: list[str], timeout: int = 20) -> str:
    """Run a fixed command safely and return combined output."""
    try:
        result = subprocess.run(
            args,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = (result.stdout or "") + (result.stderr or "")
        return output.strip()
    except Exception as exc:
        return f"[COMMAND ERROR] {exc}"


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def should_skip(path: Path) -> bool:
    return any(part in SKIP_DIRS for part in path.parts)


def iter_files():
    """Yield repository files while avoiding heavy/generated directories."""
    try:
        for path in ROOT.rglob("*"):
            if not path.is_file():
                continue
            if should_skip(path):
                continue
            yield path
    except Exception as exc:
        print(f"[SCAN WARNING] {exc}")


def safe_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return -1


def human_size(size: int) -> str:
    if size < 0:
        return "unknown"

    units = ["B", "KB", "MB", "GB"]
    value = float(size)

    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024

    return f"{size} B"


def is_probably_text(path: Path) -> bool:
    try:
        if safe_size(path) > 2_000_000:
            return False

        with path.open("rb") as f:
            sample = f.read(4096)

        if b"\x00" in sample:
            return False

        return True
    except Exception:
        return False


def print_repo_identity() -> None:
    section("1. REPOSITORY IDENTITY")

    print(f"Repository root : {ROOT}")
    print(f"Current time    : {datetime.now().isoformat(timespec='seconds')}")

    print("\nGit root:")
    print(run_cmd(["git", "rev-parse", "--show-toplevel"]))

    print("\nCurrent branch:")
    print(run_cmd(["git", "branch", "--show-current"]))

    print("\nHEAD:")
    print(run_cmd(["git", "rev-parse", "--short", "HEAD"]))


def print_git_status() -> None:
    section("2. GIT STATUS")

    print(run_cmd(["git", "status", "--short"]))

    print("\nRecent commits:")
    print(run_cmd(["git", "log", "-8", "--oneline", "--decorate"]))


def print_git_data_files() -> None:
    section("3. GIT-TRACKED DATA FILES")

    output = run_cmd(["git", "ls-files"])

    if not output or output.startswith("[COMMAND ERROR]"):
        print(output or "No tracked files found.")
        return

    matches = []

    for line in output.splitlines():
        path = Path(line)
        suffix = path.suffix.lower()

        if suffix in DATA_EXTENSIONS:
            matches.append(line)

    if matches:
        for item in matches:
            print(item)
    else:
        print("NO TRACKED HISTORICAL/DATA FILES FOUND.")
        print(
            "Checked extensions: "
            + ", ".join(sorted(DATA_EXTENSIONS))
        )


def print_all_data_candidates() -> None:
    section("4. DATA FILE CANDIDATES IN WORKSPACE")

    candidates = []

    for path in iter_files():
        if path.suffix.lower() in DATA_EXTENSIONS:
            candidates.append(path)

    candidates.sort(key=lambda p: str(p).lower())

    if not candidates:
        print("NO DATA-FORMAT FILES FOUND.")
        return

    for path in candidates:
        print(
            f"{relative(path)}"
            f"    [{human_size(safe_size(path))}]"
        )

    print(f"\nTotal candidates: {len(candidates)}")


def print_directory_snapshot() -> None:
    section("5. IMPORTANT DIRECTORY SNAPSHOT")

    important = [
        "nifty_3min_dataset",
        "gsr_data",
        ".github",
        ".devcontainer",
    ]

    for name in important:
        path = ROOT / name

        print(f"\n[{name}]")

        if not path.exists():
            print("  MISSING")
            continue

        if path.is_file():
            print("  FILE")
            continue

        children = []

        try:
            for child in path.iterdir():
                children.append(child)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            continue

        if not children:
            print("  EMPTY")
            continue

        for child in sorted(children, key=lambda p: p.name.lower())[:80]:
            marker = "/" if child.is_dir() else ""
            print(f"  {child.name}{marker}")

        if len(children) > 80:
            print(f"  ... and {len(children) - 80} more")


def print_gsr_files() -> None:
    section("6. GSR FILE INVENTORY")

    files = []

    for path in iter_files():
        name = path.name.lower()

        if (
            name.startswith("gsr")
            or "strategy_registry" in name
            or "strategy" in name and path.suffix == ".txt"
        ):
            files.append(path)

    files.sort(key=lambda p: str(p).lower())

    if not files:
        print("No obvious GSR files found.")
        return

    for path in files:
        print(
            f"{relative(path)}"
            f"    [{human_size(safe_size(path))}]"
        )


def print_replay_artifacts() -> None:
    section("7. GSR REPLAY ARTIFACTS")

    replay_root = ROOT / "gsr_data"

    if not replay_root.exists():
        print("gsr_data/ DOES NOT EXIST.")
        return

    files = []

    try:
        for path in replay_root.rglob("*"):
            if path.is_file() and not should_skip(path):
                files.append(path)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return

    files.sort(key=lambda p: str(p).lower())

    if not files:
        print("gsr_data exists but contains no files.")
        return

    for path in files[:150]:
        print(
            f"{relative(path)}"
            f"    [{human_size(safe_size(path))}]"
        )

    if len(files) > 150:
        print(f"... {len(files) - 150} additional files omitted.")


def search_code_references() -> None:
    section("8. CODE REFERENCES TO DATA / HISTORICAL REPLAY")

    results = []
    files_checked = 0

    for path in iter_files():
        if path.suffix.lower() not in CODE_EXTENSIONS:
            continue

        if not is_probably_text(path):
            continue

        files_checked += 1

        try:
            text = path.read_text(
                encoding="utf-8",
                errors="replace",
            )
        except Exception:
            continue

        lines = text.splitlines()

        for line_no, line in enumerate(lines, start=1):
            lower = line.lower()

            if any(term.lower() in lower for term in SEARCH_TERMS):
                clean = line.strip()

                if not clean:
                    continue

                results.append(
                    (
                        relative(path),
                        line_no,
                        clean[:240],
                    )
                )

    print(f"Text/code files checked: {files_checked}")
    print(f"Matching references   : {len(results)}")

    if not results:
        print("NO RELEVANT CODE REFERENCES FOUND.")
        return

    # Keep output manageable.
    for path, line_no, text in results[:250]:
        print(f"{path}:{line_no}: {text}")

    if len(results) > 250:
        print(
            f"\n... {len(results) - 250} additional matches omitted."
        )


def inspect_replay_help() -> None:
    section("9. HISTORICAL REPLAY CLI")

    replay = ROOT / "gsr_historical_replay.py"

    if not replay.exists():
        print("gsr_historical_replay.py NOT FOUND.")
        return

    print("File:")
    print(f"  {relative(replay)}")
    print(f"  Size: {human_size(safe_size(replay))}")

    print("\nCLI help:")
    output = run_cmd(
        ["python", str(replay), "--help"],
        timeout=30,
    )

    print(output[:12000])


def inspect_python_modules() -> None:
    section("10. PYTHON MODULE INVENTORY")

    files = []

    for path in iter_files():
        if path.suffix.lower() == ".py":
            files.append(path)

    files.sort(key=lambda p: str(p).lower())

    print(f"Python files found: {len(files)}")

    for path in files[:200]:
        print(
            f"{relative(path)}"
            f"    [{human_size(safe_size(path))}]"
        )

    if len(files) > 200:
        print(f"... {len(files) - 200} omitted.")


def check_ignored_paths() -> None:
    section("11. GIT IGNORE CHECK")

    gitignore = ROOT / ".gitignore"

    if not gitignore.exists():
        print(".gitignore: NOT PRESENT")
    else:
        print(".gitignore: PRESENT")
        print(gitignore.read_text(
            encoding="utf-8",
            errors="replace",
        )[:12000])

    print("\nCheck whether important directories are ignored:")

    for item in [
        "nifty_3min_dataset",
        "gsr_data",
        "gsr_historical_replay.py",
    ]:
        output = run_cmd(
            ["git", "check-ignore", "-v", "--", item]
        )

        if output:
            print(f"{item}: IGNORED")
            print(f"  {output}")
        else:
            print(f"{item}: NOT IGNORED")


def inspect_git_recent_changes() -> None:
    section("12. RECENT GSR-RELATED COMMITS")

    output = run_cmd(
        [
            "git",
            "log",
            "--all",
            "--oneline",
            "--decorate",
            "--max-count=50",
            "--",
            "*gsr*",
        ]
    )

    if output:
        print(output)
    else:
        print("No GSR-filtered commit history returned.")


def final_assessment() -> None:
    section("13. AUTOMATED ASSESSMENT")

    data_files = []

    for path in iter_files():
        if path.suffix.lower() in DATA_EXTENSIONS:
            data_files.append(path)

    nifty_dir = ROOT / "nifty_3min_dataset"
    gsr_dir = ROOT / "gsr_data"
    replay_file = ROOT / "gsr_historical_replay.py"

    print("Repository/data state:")

    if data_files:
        print(
            f"  [FOUND] {len(data_files)} data-format file(s) "
            "exist somewhere in workspace."
        )
    else:
        print(
            "  [MISSING] No recognised historical/data-format "
            "files found in workspace."
        )

    if nifty_dir.exists():
        try:
            contents = list(nifty_dir.rglob("*"))
            actual_files = [p for p in contents if p.is_file()]
        except Exception:
            actual_files = []

        if actual_files:
            print(
                f"  [FOUND] nifty_3min_dataset contains "
                f"{len(actual_files)} file(s)."
            )
        else:
            print(
                "  [EMPTY] nifty_3min_dataset contains no files."
            )
    else:
        print(
            "  [MISSING] nifty_3min_dataset directory does not exist."
        )

    if gsr_dir.exists():
        print("  [FOUND] gsr_data directory exists.")
    else:
        print("  [MISSING] gsr_data directory does not exist.")

    if replay_file.exists():
        print(
            "  [READY] gsr_historical_replay.py exists."
        )
    else:
        print(
            "  [MISSING] gsr_historical_replay.py not found."
        )

    print()
    print("IMPORTANT:")
    print(
        "Synthetic replay/self-test output is NOT treated as "
        "historical strategy evidence."
    )
    print(
        "This diagnostic does not modify the repository."
    )


def main() -> int:
    print("=" * 78)
    print("GSR REPOSITORY + HISTORICAL DATA DIAGNOSTIC")
    print("=" * 78)
    print("MODE: READ-ONLY")
    print(f"ROOT: {ROOT}")

    try:
        print_repo_identity()
        print_git_status()
        print_git_data_files()
        print_all_data_candidates()
        print_directory_snapshot()
        print_gsr_files()
        print_replay_artifacts()
        search_code_references()
        inspect_replay_help()
        inspect_python_modules()
        check_ignored_paths()
        inspect_git_recent_changes()
        final_assessment()

    except KeyboardInterrupt:
        print("\n\nDiagnostic interrupted by user.")
        return 130
    except Exception as exc:
        print("\n\nUNEXPECTED DIAGNOSTIC ERROR:")
        print(repr(exc))
        return 1

    print()
    print("=" * 78)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 78)
    print(
        "Ab ab poora output bhejna zaroori nahi hai; "
        "agar output bahut bada ho to last ~150-200 lines ka screenshot "
        "bhej dena, ya output ko text file mein save kar sakte hain."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
