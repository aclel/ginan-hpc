#!/usr/bin/env python3
"""
Submit PBS array jobs to run ginan based on available RINEX files.

Scans work_root to discover (date, station) pairs with downloaded RINEX files,
creates a manifest CSV, and submits PBS array jobs.

Must be run from the repository root directory (where pea_array.pbs exists).

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
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import csv
from utils import read_stations_from_file


def parse_size_mb(size_str: str) -> int:
    """Parse a size string like '3GB', '3000MB', '3g' into integer MB."""
    s = size_str.strip().upper().rstrip("B")
    if s.endswith("G"):
        return int(float(s[:-1]) * 1024)
    if s.endswith("M"):
        return int(float(s[:-1]))
    if s.endswith("K"):
        return max(1, int(float(s[:-1]) / 1024))
    return int(float(s))  # assume MB if no suffix


def daterange(start_date: str, end_date: str):
    """Generate dates between start and end (inclusive)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start

    for i in range(delta.days + 1):
        yield (start + timedelta(days=i)).strftime("%Y-%m-%d")


def scan_work_root(work_root: Path, allowed_stations: set[str] = None) -> list[tuple[str, str]]:
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
    for date_dir in sorted(work_root.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")):
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


def write_manifest(work_root: Path, ready: list[tuple[str, str]]) -> int:
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


def load_manifest(manifest_path: Path) -> list[tuple[str, str]]:
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


def filter_manifest_by_dates(ready: list[tuple[str, str]], start_date: str = None, end_date: str = None) -> list[tuple[str, str]]:
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


def submit_job(template: str, variables: dict, array_spec: str = None, job_name: str = None, depend_on: str = None, mem: str = None, output_path: str = None) -> str:
    """Submit a PBS job and return the job ID."""
    cmd = ["qsub"]

    # Add job name if specified
    if job_name:
        cmd.extend(["-N", job_name])

    # Add resource allocation (memory)
    if mem:
        cmd.extend(["-l", f"select=1:ncpus=1:mem={mem}"])

    # Add array specification if specified
    if array_spec:
        cmd.extend(["-J", array_spec])

    # Add PBS output path (stdout/stderr safety-net log). Trailing slash means
    # PBS will append its default name (jobname.o<jobid>.<array_index>).
    if output_path:
        cmd.extend(["-o", output_path])

    # Add variables
    var_string = ",".join([f"{k}={v}" for k, v in variables.items()])
    cmd.extend(["-v", var_string])

    # Add dependency if specified (for array jobs, add [] suffix)
    if depend_on:
        depend_on_formatted = f"{depend_on}[]"
        cmd.extend(["-W", f"depend=afterok:{depend_on_formatted}"])

    # Add template
    cmd.append(template)

    # Submit and capture job ID
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    job_id = result.stdout.strip()

    return job_id


def group_by_date(ready: list[tuple[str, str]]) -> dict[str, list[str]]:
    """
    Group (date, station) pairs by date.
    Returns dict: {date: [station1, station2, ...]}
    """
    grouped = {}
    for date, station in ready:
        if date not in grouped:
            grouped[date] = []
        grouped[date].append(station)
    return grouped


def write_date_manifest(work_root: Path, date: str, stations: list[str]) -> Path:
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


def main():
    parser = argparse.ArgumentParser(
        description="Submit PBS array jobs to run ginan on available RINEX files (one array per date)"
    )
    parser.add_argument("--work-root", type=Path, required=True, help="Base directory for work data")
    parser.add_argument("--parquet-output-dir", type=Path, required=True,
                        help="Directory to save parquet outputs (e.g., ~/parquet_outputs)")
    parser.add_argument("--config-file", type=Path, required=True,
                        help="Ginan config file to use (e.g., ppp_example_gps.yaml)")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd(),
                        help="Repository root directory (default: current directory)")
    parser.add_argument("--stations-file", type=Path, default=None,
                        help="File containing station codes to process (one per line). If not specified, all stations with RINEX will be processed.")
    parser.add_argument("--submit-start-date", default=None,
                        help="Only submit from this date (YYYY-MM-DD), optional. Filters from manifest.")
    parser.add_argument("--submit-end-date", default=None,
                        help="Only submit until this date (YYYY-MM-DD), optional. Filters from manifest.")
    parser.add_argument("--regenerate-manifest", action="store_true",
                        help="Force rescan work_root and regenerate manifest")
    parser.add_argument("--manifest-file", type=Path, default=None,
                        help="Use this manifest CSV instead of <work-root>/ready_to_process.csv "
                             "(e.g. failed_to_process.csv from check_postrun.py). Does not overwrite "
                             "the original manifest.")
    parser.add_argument("--throttle", type=int, default=None,
                        help="Max concurrent tasks per date (default: None, let PBS manage scheduling)")
    parser.add_argument("--scratch-dir", type=str, default=None,
                        help="Override scratch directory location (e.g., /dev/shm or /data/scratch). "
                             "If unset, pea_array.pbs prefers /dev/shm when it has enough free space, else $TMPDIR.")
    parser.add_argument("--scratch-size", type=str, default="5GB",
                        help="Estimated scratch space needed per job (default: 5GB). Used as the free-space "
                             "threshold for the /dev/shm check AND must be included in --mem (tmpfs writes count against cgroup mem).")
    parser.add_argument("--mem", type=str, default="3GB",
                        help="Memory allocation per job (default: 3GB). Should cover peak RSS + --scratch-size.")
    parser.add_argument("--depend-on", default=None,
                        help="PBS job ID to depend on (job will wait for this to complete)")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without submitting")

    args = parser.parse_args()

    # Validate inputs
    if not args.work_root.exists():
        print(f"ERROR: Work root not found: {args.work_root}")
        return 1

    # Validate config file
    if not args.config_file.exists():
        print(f"ERROR: Config file not found: {args.config_file}")
        return 1

    # Create parquet output directory if it doesn't exist
    args.parquet_output_dir.mkdir(parents=True, exist_ok=True)

    # Validate repo_root
    if not args.repo_root.exists():
        print(f"ERROR: Repository root not found: {args.repo_root}")
        return 1

    # Check for required scripts
    save_outputs_script = args.repo_root / "scripts" / "save_outputs_parquet.py"
    if not save_outputs_script.exists():
        print(f"ERROR: scripts/save_outputs_parquet.py not found under {args.repo_root}")
        return 1

    patch_config_script = args.repo_root / "scripts" / "patch_config.py"
    if not patch_config_script.exists():
        print(f"ERROR: scripts/patch_config.py not found under {args.repo_root}")
        return 1

    # Template path (relative to repo_root)
    pea_template = args.repo_root / "jobs" / "pea_array.pbs"
    if not pea_template.exists():
        print(f"ERROR: Template not found: {pea_template}")
        return 1

    # Load station filter if specified
    allowed_stations = None
    if args.stations_file:
        if not args.stations_file.exists():
            print(f"ERROR: Stations file not found: {args.stations_file}")
            return 1
        stations_list = read_stations_from_file(args.stations_file)
        allowed_stations = set(s.upper() for s in stations_list)  # Convert to uppercase set for faster lookup
        print(f"Loaded {len(allowed_stations)} stations from {args.stations_file}")

    # --manifest-file and --regenerate-manifest are mutually exclusive — one
    # means "use this alternative file", the other means "rebuild from scratch".
    if args.manifest_file and args.regenerate_manifest:
        print("ERROR: --manifest-file and --regenerate-manifest are mutually exclusive")
        return 1

    manifest_path = args.manifest_file or (args.work_root / "ready_to_process.csv")

    # Load or regenerate manifest
    if args.regenerate_manifest or not manifest_path.exists():
        print(f"Scanning {args.work_root} for RINEX files...")
        if allowed_stations:
            print(f"  Filtering to {len(allowed_stations)} stations from {args.stations_file}")
        ready = scan_work_root(args.work_root, allowed_stations)
        if not ready:
            if allowed_stations:
                print("No RINEX files found for specified stations in work_root")
            else:
                print("No RINEX files found in work_root")
            return 0
        num_total = write_manifest(args.work_root, ready)
        print(f"Discovered {num_total} (date, station) pairs and saved to manifest")
    else:
        ready = load_manifest(manifest_path)
        num_total = len(ready)
        print(f"Loaded manifest with {num_total} (date, station) pairs from {manifest_path}")
        if allowed_stations:
            print(f"WARNING: Using existing manifest. Station filtering only applies when regenerating manifest.")
            print(f"  Use --regenerate-manifest to apply station filter from {args.stations_file}")

    # Filter by submission date range
    to_submit = filter_manifest_by_dates(ready, args.submit_start_date, args.submit_end_date)
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
            date_range_str += (f" to {args.submit_end_date}" if date_range_str else f"until {args.submit_end_date}")
        print(f"\nFiltered to submit {date_range_str}:")
        print(f"  Total in manifest: {num_total}")
        print(f"  Will submit: {num_submit} tasks across {num_dates} dates")
    else:
        print(f"\nWill submit all {num_submit} tasks across {num_dates} dates:")

    if not to_submit:
        print("No entries matched the submission criteria")
        return 0

    # Show sample dates and station counts
    print("\nDates to process:")
    sorted_dates = sorted(grouped_by_date.keys())
    for i, date in enumerate(sorted_dates[:5]):
        num_stations = len(grouped_by_date[date])
        print(f"  {date}: {num_stations} station{'s' if num_stations != 1 else ''}")
    if num_dates > 5:
        print(f"  ... and {num_dates - 5} more dates")

    print(f"\nOriginal manifest: {manifest_path}")

    # Base PBS variables (common to all date-specific jobs)
    base_pea_vars = {
        "WORK_ROOT": str(args.work_root.resolve()),
        "PARQUET_OUTPUT_DIR": str(args.parquet_output_dir.resolve()),
        "REPO_ROOT": str(args.repo_root.resolve()),
        "CONFIG_FILE": str(args.config_file.resolve()),
    }

    # Add scratch directory override if specified
    if args.scratch_dir:
        base_pea_vars["SCRATCH_DIR_OVERRIDE"] = args.scratch_dir

    # Add memory allocation
    base_pea_vars["MEM_ALLOC"] = args.mem

    # Parse --scratch-size to integer MB for the pea_array.pbs free-space check
    required_scratch_mb = parse_size_mb(args.scratch_size)
    base_pea_vars["REQUIRED_SCRATCH_MB"] = str(required_scratch_mb)

    print(f"\nConfig file: {args.config_file.resolve()}")
    print(f"Parquet outputs: {args.parquet_output_dir.resolve()}")
    print(f"Memory per job: {args.mem}")
    print(f"Scratch size threshold: {args.scratch_size} ({required_scratch_mb}MB)")
    if args.scratch_dir:
        print(f"Scratch directory (override): {args.scratch_dir}")
    if args.throttle:
        print(f"Throttle: max {args.throttle} concurrent tasks per date")

    # Submit one array job per date
    submitted_jobs = []

    for date in sorted_dates:
        stations = grouped_by_date[date]
        num_stations = len(stations)

        # Create per-date manifest
        date_manifest = write_date_manifest(args.work_root, date, stations)

        # Array specification for this date. PBS rejects single-task arrays
        # (`-J 1-1`), so for num_stations == 1 we submit as a regular job and
        # let pea_array.pbs default PBS_ARRAY_INDEX to 1.
        if num_stations == 1:
            array_spec = None
        elif args.throttle:
            array_spec = f"1-{num_stations}%{args.throttle}"
        else:
            array_spec = f"1-{num_stations}"

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
            print(f"\n[DRY RUN] Would submit for {date} ({num_stations} stations):")
            cmd = ["qsub"]
            cmd.extend(["-N", job_name])
            if array_spec:
                cmd.extend(["-J", array_spec])
            cmd.extend(["-l", f"select=1:ncpus=1:mem={args.mem}"])
            cmd.extend(["-o", pbs_output_path])
            if args.depend_on:
                cmd.extend(["-W", f"depend=afterok:{args.depend_on}[]"])
            var_string = ",".join([f"{k}={v}" for k, v in pea_vars.items()])
            cmd.extend(["-v", var_string])
            cmd.append(str(pea_template.resolve()))
            print("  " + " ".join(cmd))
            print(f"  Manifest: {date_manifest}")
        else:
            try:
                job_id = submit_job(
                    str(pea_template),
                    pea_vars,
                    array_spec=array_spec,
                    job_name=job_name,
                    depend_on=args.depend_on,
                    mem=args.mem,
                    output_path=pbs_output_path
                )
                submitted_jobs.append((date, job_id, num_stations))
                print(f"Submitted {date}: {job_id} ({num_stations} stations)")
            except subprocess.CalledProcessError as e:
                print(f"ERROR: Failed to submit job for {date}: {e}")
                if e.stderr:
                    print(f"PBS stderr: {e.stderr.strip()}")
                if e.stdout:
                    print(f"PBS stdout: {e.stdout.strip()}")
                return 1

    if args.dry_run:
        print(f"\n[DRY RUN] Would submit {num_dates} array jobs total")
        return 0

    # Summary
    print(f"\n{'='*60}")
    print(f"Submitted {num_dates} array jobs ({num_submit} total tasks)")
    print(f"{'='*60}")
    for date, job_id, num_stations in submitted_jobs:
        print(f"  {date}: {job_id} ({num_stations} tasks)")

    print(f"\nMonitor with: qstat -u $USER")
    print(f"Check logs: tail -f {args.work_root}/*/logs/*.log")
    return 0


if __name__ == "__main__":
    exit(main())
