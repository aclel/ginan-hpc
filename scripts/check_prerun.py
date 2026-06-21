#!/usr/bin/env python3
"""
Pre-run input validation for Ginan GNSS PPP batch processing.

Checks every date in the requested range and reports a verdict:
  OK    - all required inputs present and verified
  WARN  - optional inputs missing (config not patched) but processable
  ERROR - required inputs missing, broken symlinks, or corrupt files

Usage:
    python check_prerun.py \\
        --work-root /data/work \\
        --stations stations.txt \\
        --start 2024-01-01 \\
        --end 2024-01-31

    # Show per-file detail for every date (not just problems):
    python check_prerun.py ... --verbose

Exit code: 0 if all dates are OK or WARN, 1 if any are ERROR.
"""

import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from patch_config import discover_product_files



# Files that must be present in products/ as real files or valid symlinks
REQUIRED_PRODUCTS_DIR = [
    "igs20.atx",
    "finals.data.iau2000.txt",
    "igs_satellite_metadata.snx",
    "IGc20.ssc",
]

# Files that must exist in products/tables/
REQUIRED_TABLES = [
    "sat_yaw_bias_rate.snx",
    "qzss_yaw_modes.snx",
    "bds_yaw_modes.snx",
    "OLOAD_GO.BLQ",
    "ALOAD_GO.BLQ",
    "opoleloadcoefcmcor.txt",
    "DE436.1950.2050",
    "igrf14coeffs.txt",
    "orography_ell_1x1.txt",
    "gpt_25.grd",
]

# Minimum file sizes for sanity checks (bytes)
MIN_SIZES = {
    "sp3": 50_000,
    "clk": 500_000,
    "bia": 5_000,
    "nav": 500_000,
    "snx": 50_000,
    "rinex": 50_000,
}

# VMF3 grid files required per day: H00/H06/H12/H18 for day, plus H00 for next day
VMF3_HOURS = ["H00", "H06", "H12", "H18"]



@dataclass
class DateResult:
    date_str: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)
    # (station, reason) pairs consumed by fix_rinex.py to drive redownloads.
    # reason ∈ {"missing", "zero-byte", "sha512_mismatch", "small"}
    rinex_to_fix: list[tuple[str, str]] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        if self.errors:
            return "ERROR"
        if self.warnings:
            return "WARN"
        return "OK"



def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def load_stations(path: Path) -> list[str]:
    stations = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            stations.append(line[:4].upper())
    return sorted(set(stations))


def _load_cddis_cache(work_root: Path, date_str: str) -> tuple[list[str], dict[str, str]]:
    """
    Load cached CDDIS directory listing and SHA512SUMS for a date.
    Returns (listing, sha512s). Either may be empty if not yet cached.
    """
    d = date.fromisoformat(date_str)
    doy = (d - date(d.year, 1, 1)).days + 1
    prefix = work_root / ".cddis_listings" / f"{d.year}{doy:03d}"

    listing: list[str] = []
    sha512s: dict[str, str] = {}

    listing_path = Path(str(prefix) + ".json")
    if listing_path.exists():
        try:
            listing = json.loads(listing_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    sha512_path = Path(str(prefix) + "_sha512.json")
    if sha512_path.exists():
        try:
            sha512s = json.loads(sha512_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    return listing, sha512s


def verify_sha512(path: Path, expected_hex: str) -> bool:
    h = hashlib.sha512()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest() == expected_hex.lower()


def check_file_size(p: Path, min_bytes: int, label: str) -> str | None:
    """Return error string if file is smaller than min_bytes, else None."""
    size = p.stat().st_size
    if size < min_bytes:
        return f"{label}: suspiciously small ({size:,} bytes, expected ≥{min_bytes:,})"
    return None



def _load_cddis_provenance(data_dir: Path) -> set[str]:
    """
    Load rinex_provenance.csv and return the set of .crx.gz filenames
    that were downloaded from CDDIS. Only these should be SHA512-checked.
    """
    prov_path = data_dir / "rinex_provenance.csv"
    if not prov_path.exists():
        return set()
    cddis_files: set[str] = set()
    try:
        with open(prov_path, newline="") as f:
            reader = __import__("csv").DictReader(f)
            for row in reader:
                if row.get("source", "").strip().lower() == "cddis":
                    cddis_files.add(row["filename"].strip())
    except Exception:
        pass
    return cddis_files


def check_rinex(date_str: str, data_dir: Path, expected_stations: list[str],
                sha512_cache: dict[str, str], cddis_listing: list[str],
                verify_sha512_flag: bool, result: DateResult) -> None:
    """Check RINEX observation files for all expected stations."""
    if not data_dir.is_dir():
        result.errors.append("data/ directory missing")
        return

    # Gather what's present — both .rnx and .crx.gz count as having RINEX
    present: dict[str, Path] = {}  # station -> file (.rnx preferred, .crx.gz accepted)

    for f in data_dir.iterdir():
        if not f.is_file():
            continue
        station = f.name[:4].upper()
        if f.name.endswith(".rnx"):
            present[station] = f
        elif f.name.endswith(".crx.gz") and station not in present:
            present[station] = f

    # Only SHA512-check files confirmed as CDDIS-sourced in the provenance log
    cddis_sourced = _load_cddis_provenance(data_dir) if verify_sha512_flag and sha512_cache else set()

    # Check each expected station
    missing: list[str] = []
    corrupt: list[str] = []

    for station in expected_stations:
        f = present.get(station)

        if f is None:
            missing.append(station)
            continue

        if f.stat().st_size == 0:
            corrupt.append(f"{station}(zero-byte)")
            result.rinex_to_fix.append((station, "zero-byte"))
            continue

        if f.stat().st_size < MIN_SIZES["rinex"]:
            result.warnings.append(
                f"RINEX {station}: small file ({f.stat().st_size:,} bytes)"
            )
            result.rinex_to_fix.append((station, "small"))

        # SHA512 — only for files confirmed as CDDIS-sourced in provenance log
        if cddis_sourced:
            crx_name = f.name if f.name.endswith(".crx.gz") else f.name.replace(".rnx", ".crx.gz")
            if crx_name in cddis_sourced:
                expected_hash = sha512_cache.get(crx_name)
                if expected_hash:
                    crx_path = data_dir / crx_name
                    if crx_path.exists():
                        if verify_sha512_flag and not verify_sha512(crx_path, expected_hash):
                            corrupt.append(f"{station}(SHA512 mismatch)")
                            result.rinex_to_fix.append((station, "sha512_mismatch"))

    if missing:
        if cddis_listing:
            # Split: stations that were available on CDDIS vs genuinely absent
            listing_stations = {f[:4].upper() for f in cddis_listing}
            should_have = sorted(s for s in missing if s in listing_stations)
            not_available = sorted(s for s in missing if s not in listing_stations)
            for s in should_have:
                result.rinex_to_fix.append((s, "missing"))
            if should_have:
                result.errors.append(
                    f"RINEX missing but available on CDDIS ({len(should_have)}): {', '.join(should_have)}"
                )
            if not_available:
                result.info.append(
                    f"RINEX not available on CDDIS ({len(not_available)}): {', '.join(not_available)}"
                )
        else:
            # No listing cached — can't distinguish; queue all for retry
            for s in sorted(missing):
                result.rinex_to_fix.append((s, "missing"))
            result.errors.append(f"RINEX missing ({len(missing)}): {', '.join(sorted(missing))}")
    if corrupt:
        result.errors.append(f"RINEX corrupt: {', '.join(corrupt)}")

    # Report stations in data/ that aren't in the expected list
    all_present = set(present)
    unexpected = all_present - set(expected_stations)
    if unexpected:
        result.info.append(f"Extra stations not in list: {', '.join(sorted(unexpected))}")

    n_ok = len(set(expected_stations) - set(missing))
    result.info.append(f"RINEX: {n_ok}/{len(expected_stations)} stations present")


def check_products(date_str: str, products_dir: Path, result: DateResult) -> None:
    """Check IGS orbit/clock/bias products and auxiliary static files."""
    if not products_dir.is_dir():
        result.errors.append("products/ directory missing")
        return

    # Dynamic products (orbit, clock, bias, nav, snx)
    found = discover_product_files(products_dir)

    product_labels = {
        "SP3": ("sp3_files", "sp3"),
        "CLK": ("clk_files", "clk"),
        "BIA": ("bsx_files", "bia"),
        "NAV": ("nav_files", "nav"),
    }

    missing_products: list[str] = []
    for label, (key, size_key) in product_labels.items():
        files = found[key]
        if not files:
            missing_products.append(label)
        else:
            # Sanity-check the first file's size
            p = products_dir / files[0]
            if p.is_file():
                err = check_file_size(p, MIN_SIZES[size_key], label)
                if err:
                    result.warnings.append(err)

    if missing_products:
        result.errors.append(f"Products missing: {', '.join(missing_products)}")
    else:
        result.info.append(
            "Products: " + " ".join(
                f"{lbl}({len(found[key])})"
                for lbl, (key, _) in product_labels.items()
            )
        )

    # Static files that must exist (symlinks OK as long as target is reachable)
    broken_static: list[str] = []
    for fname in REQUIRED_PRODUCTS_DIR:
        p = products_dir / fname
        if p.is_symlink():
            if not p.exists():  # broken symlink
                broken_static.append(f"{fname}(broken symlink)")
        elif not p.exists():
            broken_static.append(fname)

    # Tables files
    tables_dir = products_dir / "tables"
    if not tables_dir.is_dir():
        broken_static.append("tables/(missing)")
    else:
        for fname in REQUIRED_TABLES:
            p = tables_dir / fname
            if p.is_symlink():
                if not p.exists():
                    broken_static.append(f"tables/{fname}(broken symlink)")
            elif not p.exists():
                broken_static.append(f"tables/{fname}")

    if broken_static:
        result.errors.append(f"Aux files missing/broken: {', '.join(broken_static)}")

    # VMF3 grids — required (vmf3 troposphere model is active)
    # Need 5 files: H00/H06/H12/H18 for the processing day + H00 for next day
    d = date.fromisoformat(date_str)
    next_day = d + timedelta(days=1)
    expected_vmf3 = (
        [f"VMF3_{d.strftime('%Y%m%d')}.{h}" for h in VMF3_HOURS]
        + [f"VMF3_{next_day.strftime('%Y%m%d')}.H00"]
    )
    vmf3_set = set(found["vmf_files"])
    missing_vmf3 = [f for f in expected_vmf3 if f not in vmf3_set]
    if missing_vmf3:
        result.errors.append(f"VMF3 grids missing: {', '.join(missing_vmf3)}")



def check_date(
    date_str: str,
    work_root: Path,
    expected_stations: list[str],
    config_name: str,
    verify_sha512_flag: bool = False,
) -> DateResult:
    result = DateResult(date_str=date_str)
    date_dir = work_root / date_str

    if not date_dir.is_dir():
        result.errors.append("date directory missing entirely")
        return result

    cddis_listing, sha512_cache = _load_cddis_cache(work_root, date_str)
    check_rinex(date_str, date_dir / "data", expected_stations, sha512_cache, cddis_listing, verify_sha512_flag, result)
    check_products(date_str, date_dir / "products", result)

    return result


def print_result(res: DateResult, verbose: bool) -> None:
    verdict = res.verdict
    if verdict == "OK" and not verbose:
        return

    verdict_label = {"OK": "OK   ", "WARN": "WARN ", "ERROR": "ERROR"}[verdict]
    print(f"{res.date_str}  {verdict_label}", end="")

    if verbose or verdict != "OK":
        for msg in res.errors:
            print(f"\n    [ERROR] {msg}", end="")
        for msg in res.warnings:
            print(f"\n    [WARN]  {msg}", end="")
        if verbose:
            for msg in res.info:
                print(f"\n    [INFO]  {msg}", end="")

    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pre-run input validation for Ginan GNSS PPP batch processing"
    )
    parser.add_argument("--work-root", required=True, type=Path,
                        help="Work root directory (e.g. /data/work)")
    parser.add_argument("--stations", required=True, type=Path,
                        help="Station list file (one 4-char code per line)")
    parser.add_argument("--start", required=True, type=date.fromisoformat,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=date.fromisoformat,
                        help="End date YYYY-MM-DD")
    parser.add_argument("--config-name", default="ppp_template.yaml",
                        help="Expected config filename in each date directory")
    parser.add_argument("--verify-sha512", action="store_true",
                        help="Verify SHA512 of .crx.gz files against CDDIS cache (only valid for CDDIS-sourced files; GA files will fail)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show details for all dates, not just problems")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel worker threads for I/O checks (default: 4)")
    args = parser.parse_args()

    work_root = args.work_root.expanduser().resolve()
    if not work_root.is_dir():
        print(f"ERROR: work-root does not exist: {work_root}", file=sys.stderr)
        return 1

    if not args.stations.is_file():
        print(f"ERROR: stations file not found: {args.stations}", file=sys.stderr)
        return 1

    expected_stations = load_stations(args.stations)
    print(f"Stations ({len(expected_stations)}): {', '.join(expected_stations)}")
    print(f"Date range: {args.start} to {args.end}")
    print()

    counts = {"OK": 0, "WARN": 0, "ERROR": 0, "SKIP": 0}
    error_dates: list[DateResult] = []
    warn_dates: list[DateResult] = []

    # Track specific issue types for the summary
    missing_rinex_dates: dict[str, list[str]] = {}   # date -> [stations]
    missing_products_dates: dict[str, list[str]] = {}  # date -> [product labels]
    broken_symlink_dates: list[str] = []
    config_warn_dates: list[str] = []

    dates = list(date_range(args.start, args.end))
    n_total = len(dates)

    def _check(d: date) -> DateResult:
        return check_date(d.isoformat(), work_root, expected_stations,
                          args.config_name, args.verify_sha512)

    def _record(res: DateResult) -> None:
        date_str = res.date_str

        # Categorise for summary
        for msg in res.errors:
            if "RINEX missing" in msg:
                stations_part = msg.split(": ", 1)[-1] if ": " in msg else ""
                missing_rinex_dates[date_str] = stations_part.split(", ") if stations_part else []
            elif "Products missing" in msg:
                labels_part = msg.split(": ", 1)[-1] if ": " in msg else ""
                labels = [x.strip() for x in labels_part.split(",") if x.strip()]
                missing_products_dates.setdefault(date_str, []).extend(labels)
            elif "VMF3 grids missing" in msg:
                missing_products_dates.setdefault(date_str, []).append("VMF3")
            elif "broken symlink" in msg or "Aux files" in msg:
                broken_symlink_dates.append(date_str)

        for msg in res.warnings:
            if "not present" in msg or "not patched" in msg:
                config_warn_dates.append(date_str)
                break

        verdict = res.verdict
        counts[verdict] = counts.get(verdict, 0) + 1
        if verdict == "ERROR":
            error_dates.append(res)
        elif verdict == "WARN":
            warn_dates.append(res)

        print_result(res, args.verbose)

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for res in ex.map(_check, dates):
                _record(res)
    else:
        for d in dates:
            _record(_check(d))

    # Summary
    print()
    print("=" * 60)
    print(f"Summary: {n_total} dates checked")
    print(f"  OK:    {counts['OK']}")
    print(f"  WARN:  {counts['WARN']}")
    print(f"  ERROR: {counts['ERROR']}")
    print()

    if missing_rinex_dates:
        print(f"RINEX issues ({len(missing_rinex_dates)} dates):")
        for date_str in sorted(missing_rinex_dates)[:20]:
            stations = missing_rinex_dates[date_str]
            print(f"  {date_str}: {', '.join(stations[:10])}"
                  + (" ..." if len(stations) > 10 else ""))
        if len(missing_rinex_dates) > 20:
            print(f"  ... and {len(missing_rinex_dates) - 20} more")
        print()
        print("  Re-download just the broken (date, station) pairs with fix_rinex.py:")
        print(f"    python fix_rinex.py \\")
        print(f"      --work-root {work_root} \\")
        print(f"      --stations {args.stations} \\")
        print(f"      --start {args.start} --end {args.end}")
        print()

    if missing_products_dates:
        print(f"Products issues ({len(missing_products_dates)} dates):")
        for date_str in sorted(missing_products_dates)[:20]:
            labels = missing_products_dates[date_str]
            # Dedupe preserving order in case both Products and VMF3 added the same label
            seen: set[str] = set()
            ordered = [x for x in labels if not (x in seen or seen.add(x))]
            print(f"  {date_str}: {', '.join(ordered)}")
        if len(missing_products_dates) > 20:
            print(f"  ... and {len(missing_products_dates) - 20} more")
        print()
        pdate_list = sorted(missing_products_dates)
        print("  Re-download products:")
        print(f"    python download_products_range.py \\")
        print(f"      --work-root {work_root} \\")
        print(f"      --start {pdate_list[0]} --end {pdate_list[-1]}")
        print()

    if broken_symlink_dates:
        print(f"Broken/missing aux files ({len(broken_symlink_dates)} dates) — check products_template/ path:")
        for date_str in sorted(broken_symlink_dates)[:10]:
            print(f"  {date_str}")
        if len(broken_symlink_dates) > 10:
            print(f"  ... and {len(broken_symlink_dates) - 10} more")
        print()

    if config_warn_dates:
        print(f"Config not patched ({len(config_warn_dates)} dates) — run patch_config.py before submitting")
        print()

    if counts["ERROR"] == 0 and counts["WARN"] == 0:
        print("All dates ready to process.")

    return 1 if counts["ERROR"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
