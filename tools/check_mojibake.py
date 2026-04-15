from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_DIRS = {
    "__pycache__",
    ".git",
    ".kilo",
    ".venv",
    "venv",
    "sessions",
}
EXCLUDED_FILES = {
    "tools/check_mojibake.py",
}

# Signatures for common mojibake classes:
# 1) UTF-8 bytes decoded as cp1252/cp1251
# 2) cp1251 mojibake pairs
MOJIBAKE_PATTERNS = (
    re.compile(r"[\u00D0\u00D1\u00C2]"),
    re.compile(r"\u0420\u040e|\u0420\u0451|\u0421\u0452|\u0421\u0402"),
    re.compile(r"\u0432\u045A|\u0440\u045F|\u040F\u0412|\u0412\u00AC"),
    re.compile(r"\?{4,}"),
)


def iter_python_files(root: Path):
    for path in root.rglob("*.py"):
        if any(part in EXCLUDED_DIRS for part in path.parts):
            continue
        rel = str(path.relative_to(root)).replace("\\", "/")
        if rel in EXCLUDED_FILES:
            continue
        yield path


def find_hits(path: Path) -> list[tuple[int, str, str]]:
    hits: list[tuple[int, str, str]] = []
    text = path.read_text(encoding="utf-8", errors="ignore")
    for line_no, line in enumerate(text.splitlines(), start=1):
        for pattern in MOJIBAKE_PATTERNS:
            match = pattern.search(line)
            if match:
                hits.append((line_no, match.group(0), line.strip()))
                break
    return hits


def main() -> int:
    had_hits = False
    for path in sorted(iter_python_files(ROOT)):
        hits = find_hits(path)
        if not hits:
            continue
        had_hits = True
        rel = path.relative_to(ROOT)
        print(f"{rel}:")
        for line_no, signature, line in hits:
            print(f"  line {line_no}: signature '{signature}' -> {line}")
    if had_hits:
        print("\nMojibake signatures found. Please fix file encoding/content.")
        return 1
    print("No mojibake signatures found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
