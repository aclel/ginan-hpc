"""
Submit PBS array jobs to run ginan based on available RINEX files.

Scans work_root to discover (date, station) pairs with downloaded RINEX files,
creates a manifest CSV, and submits PBS array jobs.

Usage:
    # Scan all RINEX and submit everything
    python submit_batch.py --work-root ~/work

    # Filter by stations file (only process stations in the file)
    python submit_batch.py --work-root ~/work --stations-file stations.txt

    # Preview what will be processed
    python submit_batch.py --work-root ~/work --dry-run

    # Submit only specific dates (from discovered manifest)
    python submit_batch.py --work-root ~/work --submit-start-date 2024-01-15 --submit-end-date 2024-01-20

    # With throttling (max 10 concurrent tasks)
    python submit_batch.py --work-root ~/work --throttle 10

    # Regenerate manifest from work_root (always scans full directory)
    python submit_batch.py --work-root ~/work --regenerate-manifest

    # Regenerate manifest with station filter
    python submit_batch.py --work-root ~/work --stations-file stations.txt --regenerate-manifest
"""

import argparse
import csv
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from utils import read_stations_from_file

logger = logging.getLogger(__name__)

# submit_batch.py always lives at <repo_root>/scripts/submit_batch.py, so the
# repo root can be derived from this file's own location instead of requiring
# the caller to run from (or point at) the right directory.
REPO_ROOT = Path(__file__).resolve().parent.parent

# station code, e.g. "IQAL"
StationCode = str
# YYYY-MM-DD
DateStr = str
ManifestEntry = tuple[DateStr, StationCode]
Manifest = list[ManifestEntry]
GroupedByDate = dict[DateStr, list[StationCode]]
PeaVars = dict[str, str]
# (date, job_id, num_stations)
SubmittedJob = tuple[DateStr, str, int]


@dataclass(frozen=True)
class Args:
    work_root: Path
    parquet_output_dir: Path
    config_file: Path
    stations_file: Path | None
    submit_start_date: str | None
    submit_end_date: str | None
    regenerate_manifest: bool
    manifest_file: Path | None
    scratch_dir: str | None
    mem: str
    dry_run: bool


def scan_work_root(
    work_root: Path, allowed_stations: set[StationCode] | None = None
) -> Manifest:
    """
    Scan work_root for (date, station) pairs with RINEX/CRX files.
    Always scans the entire directory tree.

    Finds both:
    - *.rnx (decompressed RINEX files)
    - *.crx.gz (compressed CRX files - will be decompressed by run_ginan.sh)

    Args:
        work_root: Base directory to scan
        allowed_stations: Optional set of station codes to filter by (uppercase)

    Returns list of (date, station) tuples sorted by date then station.
    """
    ready = []

    # Scan all subdirectories that look like dates (YYYY-MM-DD)
    dates_to_scan = []
    for date_dir in sorted(
        work_root.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")
    ):
        if date_dir.is_dir():
            dates_to_scan.append(date_dir.name)

    # For each date, find RINEX/CRX files
    for date in dates_to_scan:
        data_dir = work_root / date / "data"
        if not data_dir.exists():
            continue

        # Track which stations we've found (prefer RNX over CRX.GZ)
        stations_found = {}  # station -> file_path

        # First, look for decompressed RNX files (preferred if both exist)
        for rinex_file in sorted(data_dir.glob("*.rnx")):
            # Extract station code: first 4 chars of filename (e.g., IQAL00CAN -> IQAL)
            station = rinex_file.stem.split("_")[0][:4].upper()
            if station not in stations_found:
                stations_found[station] = rinex_file

        # Then, look for compressed CRX.GZ files (only if no RNX exists)
        for crx_file in sorted(data_dir.glob("*.crx.gz")):
            # Extract station code: first 4 chars of filename (e.g., IQAL00CAN -> IQAL)
            station = crx_file.name.split("_")[0][:4].upper()
            if station not in stations_found:  # Only add if no RNX already found
                stations_found[station] = crx_file

        # Add stations to ready list, filtering by allowed_stations if specified
        for station in stations_found:
            if allowed_stations is None or station in allowed_stations:
                ready.append((date, station))

    # Sort by date, then station
    ready.sort()

    return ready


def write_manifest(work_root: Path, ready: Manifest) -> int:
    """
    Write ready_to_process.csv manifest to work_root.
    Returns number of entries written.
    """
    manifest_path = work_root / "ready_to_process.csv"

    with open(manifest_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "station"])  # Header
        writer.writerows(ready)

    return len(ready)


def load_manifest(manifest_path: Path) -> Manifest:
    """
    Load a manifest CSV at the given path.
    Returns list of (date, station) tuples.
    """
    if not manifest_path.exists():
        return []

    ready = []
    with open(manifest_path, "r", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row:  # Skip empty rows
                ready.append((row[0], row[1]))

    return ready


def filter_manifest_by_dates(
    ready: Manifest, start_date: str | None = None, end_date: str | None = None
) -> Manifest:
    """
    Filter manifest entries by date range.
    Returns filtered list of (date, station) tuples.
    """
    if not start_date and not end_date:
        return ready

    # Convert to datetime for comparison
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.min
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.max

    filtered = []
    for date, station in ready:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        if start <= date_obj <= end:
            filtered.append((date, station))

    return filtered


def group_by_date(ready: Manifest) -> GroupedByDate:
    """
    Group (date, station) pairs by date.
    Returns dict: {date: [station1, station2, ...]}
    """
    grouped: GroupedByDate = {}
    for date, station in ready:
        if date not in grouped:
            grouped[date] = []
        grouped[date].append(station)
    return grouped


def write_date_manifest(
    work_root: Path, date: DateStr, stations: list[StationCode]
) -> Path:
    """
    Write per-date manifest: manifest_{date}.csv
    Returns path to manifest file.
    """
    manifest_path = work_root / f"manifest_{date}.csv"

    with open(manifest_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["station"])  # Header (date is implicit in filename)
        for station in stations:
            writer.writerow([station])

    return manifest_path


def submit_job(
    template: str,
    variables: PeaVars,
    array_spec: str | None = None,
    job_name: str | None = None,
    depend_on: str | None = None,
    mem: str | None = None,
    output_path: str | None = None,
) -> str:
    """Submit a PBS job and return the job ID."""
    cmd = ["qsub"]

    if job_name:
        cmd.extend(["-N", job_name])

    if mem:
        cmd.extend(["-l", f"select=1:ncpus=1:mem={mem}"])

    if array_spec:
        cmd.extend(["-J", array_spec])

    # PBS output path (stdout/stderr safety-net log). Trailing slash means
    # PBS will append its default name (jobname.o<jobid>.<array_index>).
    if output_path:
        cmd.extend(["-o", output_path])

    var_string = ",".join([f"{k}={v}" for k, v in variables.items()])
    cmd.extend(["-v", var_string])

    # Dependency (for array jobs, add [] suffix)
    if depend_on:
        cmd.extend(["-W", f"depend=afterok:{depend_on}[]"])

    cmd.append(template)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd, result.stdout, result.stderr
        )

    return result.stdout.strip()


def parse_args() -> Args:
    parser = argparse.ArgumentParser(
        description="Submit PBS array jobs to run ginan on available RINEX files (one array per date)"
    )
    parser.add_argument(
        "--work-root", type=Path, required=True, help="Base directory for work data"
    )
    parser.add_argument(
        "--parquet-output-dir",
        type=Path,
        required=True,
        help="Directory to save parquet outputs (e.g., ~/parquet_outputs)",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        required=True,
        help="Ginan config file to use (e.g., ppp_example_gps.yaml)",
    )
    parser.add_argument(
        "--stations-file",
        type=Path,
        default=None,
        help="File containing station codes to process (one per line). If not specified, all stations with RINEX will be processed.",
    )
    parser.add_argument(
        "--submit-start-date",
        default=None,
        help="Only submit from this date (YYYY-MM-DD), optional. Filters from manifest.",
    )
    parser.add_argument(
        "--submit-end-date",
        default=None,
        help="Only submit until this date (YYYY-MM-DD), optional. Filters from manifest.",
    )
    parser.add_argument(
        "--regenerate-manifest",
        action="store_true",
        help="Force rescan work_root and regenerate manifest",
    )
    parser.add_argument(
        "--manifest-file",
        type=Path,
        default=None,
        help="Use this manifest CSV instead of <work-root>/ready_to_process.csv "
        "(e.g. failed_to_process.csv from check_postrun.py). Does not overwrite "
        "the original manifest.",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Override scratch directory location (e.g., /dev/shm or /data/scratch). "
        "If unset, pea_array.pbs prefers /dev/shm when it has enough free space, else $TMPDIR.",
    )
    parser.add_argument(
        "--mem",
        type=str,
        default="3GB",
        help="Memory allocation per job (default: 3GB). Should cover peak RSS + --scratch-size.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without submitting"
    )

    namespace = parser.parse_args()
    return Args(**vars(namespace))


def validate_args(args: Args) -> Path:
    """Validate paths and mutually-exclusive flags. Returns the pea_array.pbs template path."""
    if not args.work_root.exists():
        raise FileNotFoundError(f"Work root not found: {args.work_root}")

    if not args.config_file.exists():
        raise FileNotFoundError(f"Config file not found: {args.config_file}")

    # Create parquet output directory if it doesn't exist
    args.parquet_output_dir.mkdir(parents=True, exist_ok=True)

    # Check for required scripts
    save_outputs_script = REPO_ROOT / "scripts" / "save_outputs_parquet.py"
    if not save_outputs_script.exists():
        raise FileNotFoundError(
            f"scripts/save_outputs_parquet.py not found under {REPO_ROOT}"
        )

    patch_config_script = REPO_ROOT / "scripts" / "patch_config.py"
    if not patch_config_script.exists():
        raise FileNotFoundError(f"scripts/patch_config.py not found under {REPO_ROOT}")

    # Template path (relative to repo_root)
    pea_template = REPO_ROOT / "jobs" / "pea_array.pbs"
    if not pea_template.exists():
        raise FileNotFoundError(f"Template not found: {pea_template}")

    # --manifest-file and --regenerate-manifest are mutually exclusive — one
    # means "use this alternative file", the other means "rebuild from scratch".
    if args.manifest_file and args.regenerate_manifest:
        raise ValueError(
            "--manifest-file and --regenerate-manifest are mutually exclusive"
        )

    return pea_template


def parse_stations_file(stations_file: Path | None) -> set[StationCode] | None:
    """Load and uppercase the station filter, if one was given."""
    if not stations_file:
        return None

    if not stations_file.exists():
        raise FileNotFoundError(f"Stations file not found: {stations_file}")

    stations_list = read_stations_from_file(stations_file)
    allowed_stations = set(
        s.upper() for s in stations_list
    )  # uppercase set for faster lookup
    logger.info("Loaded %d stations from %s", len(allowed_stations), stations_file)

    return allowed_stations


def load_or_build_manifest(
    work_root: Path,
    regenerate_manifest: bool,
    manifest_path: Path,
    allowed_stations: set[StationCode] | None,
) -> tuple[Manifest, int]:
    """Scan work_root and (re)write the manifest, or load an existing one."""
    if regenerate_manifest or not manifest_path.exists():
        logger.info("Scanning %s for RINEX files...", work_root)
        if allowed_stations:
            logger.info("  Filtering to %d stations", len(allowed_stations))
        ready = scan_work_root(work_root, allowed_stations)
        num_total = write_manifest(work_root, ready)
        logger.info(
            "Discovered %d (date, station) pairs and saved to manifest", num_total
        )
    else:
        ready = load_manifest(manifest_path)
        num_total = len(ready)
        logger.info(
            "Loaded manifest with %d (date, station) pairs from %s",
            num_total,
            manifest_path,
        )
        if allowed_stations:
            logger.warning(
                "Using existing manifest. Station filtering only applies when regenerating manifest."
            )
            logger.warning("  Use --regenerate-manifest to apply the station filter")

    return ready, num_total


def build_base_pea_vars(args: Args) -> PeaVars:
    base_pea_vars: PeaVars = {
        "WORK_ROOT": str(args.work_root.resolve()),
        "PARQUET_OUTPUT_DIR": str(args.parquet_output_dir.resolve()),
        "REPO_ROOT": str(REPO_ROOT),
        "CONFIG_FILE": str(args.config_file.resolve()),
        "MEM_ALLOC": args.mem,
    }
    if args.scratch_dir:
        base_pea_vars["SCRATCH_DIR_OVERRIDE"] = args.scratch_dir
    return base_pea_vars


def submit_all(
    args: Args,
    pea_template: Path,
    sorted_dates: list[DateStr],
    grouped_by_date: GroupedByDate,
    base_pea_vars: PeaVars,
) -> list[SubmittedJob]:
    submitted_jobs: list[SubmittedJob] = []

    for date in sorted_dates:
        stations = grouped_by_date[date]
        num_stations = len(stations)

        # Create per-date manifest
        date_manifest = write_date_manifest(args.work_root, date, stations)

        # Array specification for this date. PBS rejects single-task arrays
        # (`-J 1-1`), so for num_stations == 1 we submit as a regular job and
        # let pea_array.pbs default PBS_ARRAY_INDEX to 1.
        array_spec = None if num_stations == 1 else f"1-{num_stations}"

        # PBS log directory (must exist at submission time for -o to work)
        logs_dir = args.work_root / date / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        # Trailing slash tells PBS to append its default filename (jobname.o<jobid>.<index>)
        pbs_output_path = f"{logs_dir.resolve()}/"

        # Per-date PBS variables (add date-specific manifest and date)
        pea_vars = base_pea_vars.copy()
        pea_vars["MANIFEST"] = str(date_manifest.resolve())
        pea_vars["DATE"] = date

        job_name = date  # Job name is just the date (e.g., "2020-06-01")

        if args.dry_run:
            logger.info(
                "[DRY RUN] Would submit for %s (%d stations):", date, num_stations
            )
            cmd = ["qsub", "-N", job_name]
            if array_spec:
                cmd.extend(["-J", array_spec])
            cmd.extend(["-l", f"select=1:ncpus=1:mem={args.mem}"])
            cmd.extend(["-o", pbs_output_path])
            var_string = ",".join(f"{k}={v}" for k, v in pea_vars.items())
            cmd.extend(["-v", var_string])
            cmd.append(str(pea_template.resolve()))
            logger.info("  %s", " ".join(cmd))
            logger.info("  Manifest: %s", date_manifest)
            continue

        try:
            job_id = submit_job(
                str(pea_template),
                pea_vars,
                array_spec=array_spec,
                job_name=job_name,
                mem=args.mem,
                output_path=pbs_output_path,
            )
        except subprocess.CalledProcessError as e:
            logger.error("Failed to submit job for %s: %s", date, e)
            if e.stderr:
                logger.error("PBS stderr: %s", e.stderr.strip())
            if e.stdout:
                logger.error("PBS stdout: %s", e.stdout.strip())
            raise

        submitted_jobs.append((date, job_id, num_stations))
        logger.info("Submitted %s: %s (%d stations)", date, job_id, num_stations)

    return submitted_jobs


def main() -> None:
    args = parse_args()
    pea_template = validate_args(args)
    allowed_stations = parse_stations_file(args.stations_file)

    manifest_path = args.manifest_file or (args.work_root / "ready_to_process.csv")
    ready, num_total = load_or_build_manifest(
        args.work_root, args.regenerate_manifest, manifest_path, allowed_stations
    )

    # Filter by submission date range
    to_submit = filter_manifest_by_dates(
        ready, args.submit_start_date, args.submit_end_date
    )
    num_submit = len(to_submit)

    # Group by date
    grouped_by_date = group_by_date(to_submit)
    num_dates = len(grouped_by_date)

    # Display what will be processed
    if args.submit_start_date or args.submit_end_date:
        date_range_str = ""
        if args.submit_start_date:
            date_range_str += f"from {args.submit_start_date}"
        if args.submit_end_date:
            date_range_str += (
                f" to {args.submit_end_date}"
                if date_range_str
                else f"until {args.submit_end_date}"
            )
        logger.info("Filtered to submit %s:", date_range_str)
        logger.info("  Total in manifest: %d", num_total)
        logger.info("  Will submit: %d tasks across %d dates", num_submit, num_dates)
    else:
        logger.info("Will submit all %d tasks across %d dates:", num_submit, num_dates)

    if not to_submit:
        logger.info("No entries matched the submission criteria")
        return

    # Show sample dates and station counts
    sorted_dates = sorted(grouped_by_date.keys())
    logger.info("Dates to process:")
    for date in sorted_dates[:5]:
        num_stations = len(grouped_by_date[date])
        logger.info(
            "  %s: %d station%s", date, num_stations, "s" if num_stations != 1 else ""
        )
    if num_dates > 5:
        logger.info("  ... and %d more dates", num_dates - 5)

    logger.info("Original manifest: %s", manifest_path)

    base_pea_vars = build_base_pea_vars(args)

    logger.info("Config file: %s", args.config_file.resolve())
    logger.info("Parquet outputs: %s", args.parquet_output_dir.resolve())
    logger.info("Memory per job: %s", args.mem)
    if args.scratch_dir:
        logger.info("Scratch directory (override): %s", args.scratch_dir)

    submitted_jobs = submit_all(
        args, pea_template, sorted_dates, grouped_by_date, base_pea_vars
    )

    if args.dry_run:
        logger.info("[DRY RUN] Would submit %d array jobs total", num_dates)
        return

    # Summary
    logger.info("=" * 60)
    logger.info("Submitted %d array jobs (%d total tasks)", num_dates, num_submit)
    logger.info("=" * 60)
    for date, job_id, num_stations in submitted_jobs:
        logger.info("  %s: %s (%d tasks)", date, job_id, num_stations)

    logger.info("Monitor with: qstat -u $USER")
    logger.info("Check logs: tail -f %s/*/logs/*.log", args.work_root)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        main()
    except Exception as e:
        logger.error("ERROR: %s", e)
        raise SystemExit(1)
