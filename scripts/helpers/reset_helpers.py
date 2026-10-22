"""Reusable helpers for guarded reset scripts."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path


def clear_directory_contents(directory: Path) -> int:
    """Delete every direct child while preserving the target directory."""
    if not directory.exists():
        return 0
    removed = 0
    for child in directory.iterdir():
        if child.is_symlink() or child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)
        removed += 1
    return removed


def confirmation_for_target(
    target_directory: Path,
    input_fn: Callable[[str], str] = input,
) -> bool:
    """Prompt for explicit y/n confirmation before destructive reset."""
    while True:
        answer = input_fn(
            f"Delete all contents inside {target_directory}? [y/n]: "
        ).strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes/y or no/n.")

