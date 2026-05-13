from __future__ import annotations

from pathlib import Path


def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def parent_dir(path: str) -> str:
    return str(Path(path).parent)


def path_exists(path: str) -> bool:
    return Path(path).exists()
