import argparse
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

INCLUDE_FILES = [
    ".gitignore",
    "README.md",
    "pyproject.toml",
]

INCLUDE_DIRS = [
    "docs",
    "scripts",
    "skills",
    "src",
    "tests",
    "tools",
]

EXCLUDE_NAMES = {
    ".git",
    ".env",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".cws",
    "state",
    "tmp",
    "backups",
    "server_backups",
}

EXCLUDE_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite3",
    ".db",
    ".log",
}


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _should_skip(path: Path) -> bool:
    return any(part in EXCLUDE_NAMES for part in path.parts) or path.suffix.lower() in EXCLUDE_SUFFIXES


def _copy_dir(src: Path, dst: Path) -> int:
    copied = 0
    if not src.exists():
        return copied
    _remove_path(dst)
    for item in src.rglob("*"):
        rel = item.relative_to(src)
        if item.is_dir() or _should_skip(rel):
            continue
        _copy_file(item, dst / rel)
        copied += 1
    return copied


def export_tree(dest: Path) -> int:
    copied = 0
    dest.mkdir(parents=True, exist_ok=True)
    for rel in INCLUDE_FILES:
        src = ROOT / rel
        dst = dest / rel
        if src.exists():
            _copy_file(src, dst)
            copied += 1
        else:
            _remove_path(dst)
    for rel in INCLUDE_DIRS:
        src = ROOT / rel
        dst = dest / rel
        if src.exists():
            copied += _copy_dir(src, dst)
        else:
            _remove_path(dst)
    return copied


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a sanitized project tree for GitHub publishing.")
    parser.add_argument("--dest", required=True, help="Destination directory, e.g. backups/push_tmp_repo")
    args = parser.parse_args()

    dest = Path(args.dest).expanduser().resolve()
    count = export_tree(dest)
    print(f"Exported {count} file(s) to {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
