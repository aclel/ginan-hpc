#!/usr/bin/env python3
"""
Post-run verification for Ginan GNSS PPP batch processing.

For every (date, station) pair in the manifest (or discovered in work_root),
checks that:
  1. The per-station log exists and contains the clean-exit marker
  2. Every expected parquet suffix is present and non-empty in the output dir

Reports a verdict per pair:
  OK    - log clean, parquet written
  WARN  - log clean but parquet thin/missing (possible but processed)
  ERROR - missing log, unclean log, or no parquet output

Writes failed_to_process.csv (same schema as ready_to_process.csv) so you
can feed it straight back to submit_batch.py for a retry pass.

Usage:
    python check_postrun.py \\
        --work-root ~/work \\
        --parquet-output-dir ~/parquet

    # Date-range subset:
    python check_postrun.py ... --start 2024-01-01 --end 2024-01-31

    # Station filter:
    python check_postrun.py ... --stations stations.txt

    # Show every pair, not just problems:
    python check_postrun.py ... --verbose

Exit code: 0 if all pairs OK, 1 if any ERROR.
"""

import argparse
import csv
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path



# Clean-exit marker written by run_ginan.sh at the very end.
SUCCESS_MARKER = re.compile(r"ginan processing completed for \S+ on \d{4}-\d{2}-\d{2}")

# Ordered list of (regex, short reason) — first match wins for diagnostics.
FAILURE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^Terminated\b"),                       "terminated (walltime or qdel)"),
    (re.compile(r"Killed"),                              "killed (OOM or signal)"),
    (re.compile(r"ERROR: ginan failed"),                 "ginan returned non-zero"),
    (re.compile(r"ERROR: Parquet conversion failed"),    "parquet conversion failed"),
    (re.compile(r"ERROR: Config patching failed"),       "config patching failed"),
    (re.compile(r"ERROR: No RINEX or CRX file"),         "no RINEX input found"),
    (re.compile(r"ERROR: Failed to decompress"),         "CRX decompression failed"),
    (re.compile(r"ERROR: Failed to find decompressed"),  "post-decompress RINEX missing"),
    (re.compile(r"ERROR:"),                              "ERROR in log"),
]

# Slow-convergence warning; not a failure by itself but worth surfacing.
FILTER_NONCONVERGENCE = re.compile(r"Max post-fit filter iterations limit reached")

# Parquet suffixes that save_outputs_parquet.py emits for a healthy run.
# Matched as "<anything>_<suffix>.parquet" — longest suffixes are checked first
# so e.g. "network_residuals_smoothed" doesn't collide with "network_residuals".
#
# Note: save_outputs_parquet.py writes either `network_residuals` OR
# `network_residuals_smoothed`, never both — the choice is driven by the
# `keep_raw_residuals` flag. The default config uses keep_raw_residuals=False,
# so `network_residuals_smoothed` is in this list. Swap it for
# `network_residuals` via --required-suffixes if you run with raw residuals.
REQUIRED_PARQUET_SUFFIXES: list[str] = [
    # Position & station-level (from TRACE + POS files)
    "pos",
    "station_observations",
    "station_pde_cs",
    "station_lc",
    "station_detslp",
    # Network-level from forward trace
    "network_residuals_smoothed",
    "network_large_errors",
    "network_ambiguity_resets",
    "network_trop",
    # Network-level from smoothed trace
    "network_trop_smoothed",
]



@dataclass
class PairResult:
    date_str: str
    station: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        if self.errors:
            return "ERROR"
        if self.warnings:
            return "WARN"
        return "OK"



def load_manifest(work_root: Path) -> list[tuple[str, str]] | None:
    """Return list from ready_to_process.csv, or None if it's absent."""
    p = work_root / "ready_to_process.csv"
    if not p.is_file():
        return None
    pairs: list[tuple[str, str]] = []
    with open(p, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            if len(row) >= 2:
                pairs.append((row[0].strip(), row[1].strip().upper()))
    return pairs


def discover_from_workdir(work_root: Path) -> list[tuple[str, str]]:
    """Fallback: scan work_root for (date, station) with RINEX or CRX files."""
    pairs: set[tuple[str, str]] = set()
    for date_dir in sorted(work_root.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")):
        data_dir = date_dir / "data"
        if not data_dir.is_dir():
            continue
        for f in data_dir.iterdir():
            if f.name.endswith(".rnx") or f.name.endswith(".crx.gz"):
                station = f.name.split("_")[0][:4].upper()
                pairs.add((date_dir.name, station))
    return sorted(pairs)


def filter_pairs(
    pairs: list[tuple[str, str]],
    start: date | None,
    end: date | None,
    stations: set[str] | None,
) -> list[tuple[str, str]]:
    out = []
    for d, s in pairs:
        if stations is not None and s not in stations:
            continue
        if start or end:
            try:
                dd = date.fromisoformat(d)
            except ValueError:
                continue
            if start and dd < start:
                continue
            if end and dd > end:
                continue
        out.append((d, s))
    return out


def load_stations_filter(path: Path) -> set[str]:
    codes: set[str] = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            codes.add(line[:4].upper())
    return codes



def check_log(log_path: Path, result: PairResult) -> bool:
    """
    Return True if log signals a clean run. Populates errors/warnings on result.
    """
    if not log_path.is_file():
        result.errors.append("log file missing")
        return False

    try:
        text = log_path.read_text(errors="replace")
    except OSError as e:
        result.errors.append(f"cannot read log: {e}")
        return False

    if not text.strip():
        result.errors.append("log file empty")
        return False

    success = bool(SUCCESS_MARKER.search(text))

    # Look at the last ~50 lines for the tell-tale failure mode.
    tail = "\n".join(text.splitlines()[-50:])
    reason: str | None = None
    for pat, label in FAILURE_PATTERNS:
        if pat.search(tail):
            reason = label
            break

    if not success:
        result.errors.append(reason or "no success marker")
        return False

    # Log was clean, but flag convergence warnings so you know which stations
    # were ragged even when "successful".
    nonconv = len(FILTER_NONCONVERGENCE.findall(text))
    if nonconv > 10:
        result.warnings.append(f"filter non-convergence ×{nonconv}")

    return True


def check_parquet(
    parquet_dir: Path,
    required_suffixes: list[str],
    result: PairResult,
) -> None:
    """Require every suffix in `required_suffixes` present as a non-empty file."""
    if not parquet_dir.is_dir():
        result.errors.append(f"parquet dir missing: {parquet_dir}")
        return

    pq_files = sorted(parquet_dir.glob("*.parquet"))
    if not pq_files:
        result.errors.append("no parquet files produced")
        return

    # Match files to required suffixes. Longest suffixes first so
    # "network_residuals_smoothed" wins over "network_residuals".
    sorted_required = sorted(required_suffixes, key=len, reverse=True)
    matched: dict[str, Path] = {}
    for f in pq_files:
        for suf in sorted_required:
            if f.name.endswith(f"_{suf}.parquet") and suf not in matched:
                matched[suf] = f
                break

    missing = [s for s in required_suffixes if s not in matched]
    if missing:
        result.errors.append(f"missing parquet suffix(es): {', '.join(missing)}")

    empty = [suf for suf, f in matched.items() if f.stat().st_size == 0]
    if empty:
        result.errors.append(f"zero-byte parquet suffix(es): {', '.join(empty)}")

    non_empty_bytes = sum(f.stat().st_size for f in matched.values() if f.stat().st_size > 0)
    result.info.append(
        f"parquet: {len(matched) - len(empty)}/{len(required_suffixes)} suffixes OK, "
        f"{non_empty_bytes/1024/1024:.1f}MB total"
    )


def check_pair(
    date_str: str,
    station: str,
    work_root: Path,
    parquet_root: Path,
    required_suffixes: list[str],
) -> PairResult:
    result = PairResult(date_str=date_str, station=station)
    log_path = work_root / date_str / "logs" / f"{station}.log"
    parquet_dir = parquet_root / date_str / station

    log_ok = check_log(log_path, result)
    # Only bother checking parquet if the log claims success — otherwise the
    # parquet issue is redundant noise on top of the real failure.
    if log_ok:
        check_parquet(parquet_dir, required_suffixes, result)

    return result



def print_result(res: PairResult, verbose: bool) -> None:
    verdict = res.verdict
    if verdict == "OK" and not verbose:
        return

    label = {"OK": "OK   ", "WARN": "WARN ", "ERROR": "ERROR"}[verdict]
    print(f"{res.date_str}  {res.station}  {label}", end="")
    for msg in res.errors:
        print(f"\n    [ERROR] {msg}", end="")
    for msg in res.warnings:
        print(f"\n    [WARN]  {msg}", end="")
    if verbose:
        for msg in res.info:
            print(f"\n    [INFO]  {msg}", end="")
    print()


def write_failed_manifest(path: Path, failed: list[PairResult]) -> int:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "station"])
        for r in failed:
            w.writerow([r.date_str, r.station])
    return len(failed)


def write_check_report(path: Path, results: list[PairResult]) -> int:
    """Write a per-pair report with verdict + errors + warnings to CWD."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "station", "verdict", "errors", "warnings"])
        for r in results:
            w.writerow([
                r.date_str,
                r.station,
                r.verdict,
                "; ".join(r.errors),
                "; ".join(r.warnings),
            ])
    return len(results)



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Post-run verification: which (date, station) pairs succeeded?"
    )
    parser.add_argument("--work-root", required=True, type=Path,
                        help="Work root (contains date dirs with logs/)")
    parser.add_argument("--parquet-output-dir", required=True, type=Path,
                        help="Parquet output root (contains date/station subdirs)")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Start date YYYY-MM-DD (optional)")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="End date YYYY-MM-DD (optional)")
    parser.add_argument("--stations", type=Path, default=None,
                        help="Station filter file (one 4-char code per line)")
    parser.add_argument("--failed-manifest", type=Path, default=None,
                        help="Path to write failed-pairs CSV (default: "
                             "<work-root>/failed_to_process.csv)")
    parser.add_argument("--no-manifest", action="store_true",
                        help="Skip ready_to_process.csv; always scan work_root")
    parser.add_argument("--required-suffixes", type=str, default=None,
                        help="Comma-separated parquet suffixes required per pair "
                             f"(default: all {len(REQUIRED_PARQUET_SUFFIXES)} suffixes)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print every pair, not just non-OK")
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel worker threads for I/O checks (default: 4)")
    args = parser.parse_args()

    work_root = args.work_root.expanduser().resolve()
    parquet_root = args.parquet_output_dir.expanduser().resolve()

    if not work_root.is_dir():
        print(f"ERROR: work-root not found: {work_root}", file=sys.stderr)
        return 1
    if not parquet_root.is_dir():
        print(f"WARNING: parquet-output-dir does not exist yet: {parquet_root}", file=sys.stderr)

    # Source of truth for the pair list
    if args.no_manifest:
        pairs = discover_from_workdir(work_root)
        source = "scanned work_root"
    else:
        pairs = load_manifest(work_root)
        if pairs is None:
            pairs = discover_from_workdir(work_root)
            source = "scanned work_root (no manifest)"
        else:
            source = "ready_to_process.csv"

    stations_filter = load_stations_filter(args.stations) if args.stations else None
    pairs = filter_pairs(pairs, args.start, args.end, stations_filter)

    if not pairs:
        print("No (date, station) pairs to check.")
        return 0

    print(f"Checking {len(pairs)} pairs (source: {source})")
    if args.start or args.end:
        print(f"  Date range: {args.start or 'any'} to {args.end or 'any'}")
    if stations_filter:
        print(f"  Station filter: {len(stations_filter)} stations")
    print()

    counts = {"OK": 0, "WARN": 0, "ERROR": 0}
    failed: list[PairResult] = []
    all_results: list[PairResult] = []
    # Tally error reasons for the summary
    reason_counts: dict[str, int] = {}

    required_suffixes = (
        [x.strip() for x in args.required_suffixes.split(",") if x.strip()]
        if args.required_suffixes
        else REQUIRED_PARQUET_SUFFIXES
    )
    print(f"  Required parquet suffixes ({len(required_suffixes)}): {', '.join(required_suffixes)}")
    print()

    start_ts = datetime.now()

    def _check(pair: tuple[str, str]) -> PairResult:
        return check_pair(pair[0], pair[1], work_root, parquet_root, required_suffixes)

    def _record(res: PairResult) -> None:
        counts[res.verdict] += 1
        all_results.append(res)
        if res.verdict == "ERROR":
            failed.append(res)
            for msg in res.errors:
                reason_counts[msg] = reason_counts.get(msg, 0) + 1
        print_result(res, args.verbose)

    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for res in ex.map(_check, pairs):
                _record(res)
    else:
        for pair in pairs:
            _record(_check(pair))

    elapsed = (datetime.now() - start_ts).total_seconds()

    print()
    print("=" * 60)
    print(f"Summary: {len(pairs)} pairs checked in {elapsed:.1f}s")
    print(f"  OK:    {counts['OK']}")
    print(f"  WARN:  {counts['WARN']}")
    print(f"  ERROR: {counts['ERROR']}")

    if reason_counts:
        print()
        print("Top failure reasons:")
        for reason, n in sorted(reason_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {n:5d}  {reason}")

    if failed:
        manifest_path = args.failed_manifest or (work_root / "failed_to_process.csv")
        n = write_failed_manifest(manifest_path, failed)
        print()
        print(f"Wrote {n} failed pairs to: {manifest_path}")
        print(f"Retry with:")
        print(f"  python submit_batch.py --work-root {work_root} \\")
        print(f"      --manifest-file {manifest_path} \\")
        print(f"      --parquet-output-dir <your parquet dir> \\")
        print(f"      --config-file <your config> ...")

    # Per-pair report in CWD with date-range suffix.
    pair_dates = sorted({d for d, _ in pairs})
    range_start = args.start.isoformat() if args.start else pair_dates[0]
    range_end = args.end.isoformat() if args.end else pair_dates[-1]
    report_path = Path.cwd() / f"check_postrun_{range_start}_{range_end}.csv"
    write_check_report(report_path, all_results)
    print()
    print(f"Wrote per-pair report ({len(all_results)} rows) to: {report_path}")

    return 1 if counts["ERROR"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
