"""Shared utility functions that don't require heavy dependencies."""

import os
import shutil
from pathlib import Path


def read_stations_from_file(stations_file: Path) -> list[str]:
    """Return non-empty, non-comment lines from a station list file."""
    with open(stations_file, 'r') as f:
        stations = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    return stations


def ensure_rinex_dir(work_root: Path, date: str) -> Path:
    """Create and return work_root/date/data/."""
    data_dir = (work_root / date / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def ensure_workdir(work_root: Path, root_dir: Path, date: str, products_template: Path) -> Path:
    """Create work_root/date/ and symlink static product files from products_template.

    root_dir is unused but kept for API compatibility with external callers.
    """
    work_dir = (work_root / date).resolve()
    products_dir = work_dir / "products"
    tables_dir = products_dir / "tables"

    products_dir.mkdir(parents=True, exist_ok=True)

    for fname in ["finals.data.iau2000.txt", "igs20.atx", "igs_satellite_metadata.snx", "IGc20.ssc"]:
        target = products_dir / fname
        if target.exists() or target.is_symlink():
            target.unlink()
        source_file = products_template / fname
        relative_path = os.path.relpath(source_file, products_dir)
        target.symlink_to(relative_path)

    if tables_dir.exists() and not tables_dir.is_dir():
        tables_dir.unlink()
    tables_dir.mkdir(parents=True, exist_ok=True)

    for f in (products_template / "tables").glob("*"):
        target = tables_dir / f.name
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(os.path.relpath(f, tables_dir))

    return work_dir
