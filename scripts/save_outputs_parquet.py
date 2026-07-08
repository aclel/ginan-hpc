#!/usr/bin/env python3
"""
Extract TRACE and POS data frames and persist them to Parquet for validation.

The script discovers TRACE or POS files under a station/date folder (or processes a
single file) and uses gnssanalysis.gn_io.trace helpers to build pandas DataFrames.

TRACE files:
- Network TRACE files yield residuals (raw/final), large-error and ambiguity-reset tables
- Station TRACE files yield PDE-CS, LC combination, and cycle slip detection (detslp) tables
- Each populated DataFrame is written to <output-dir>/<basename>_<kind>.parquet

POS files:
- Ginan POS files (time series position solutions) are parsed into a single DataFrame
- Output written to <output-dir>/<basename>_pos.parquet
"""

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Literal, cast, overload

import pandas as pd

from gnssanalysis.gn_io import trace

logger = logging.getLogger(__name__)

ParquetEngine = Literal["auto", "pyarrow", "fastparquet"]


@dataclass(slots=True)
class RunConfig:
    output_dir: Path
    engine: str
    keep_raw_residuals: bool
    quiet: bool


@dataclass(frozen=True)
class Args:
    target: Path
    output_dir: Path
    engine: str
    keep_raw_residuals: bool
    recursive: bool
    quiet: bool


_STATION_PREFIX_RE = re.compile(r"^[A-Z0-9]{4}")

# POS file lines to skip during parsing (header, column labels, comments)
_POS_HEADER_PREFIXES = (
    "#",
    "*",
    "PBO",
    "Format",
    "4-character",
    "First",
    "XYZ",
    "NEU",
    "Start",
    "End",
    "YYYY",
    "X ",
    "Y ",
    "Z ",
    "Sx",
    "Sy",
    "Sz",
    "Rxy",
    "Rxz",
    "Ryz",
    "Nlat",
    "Elong",
    "Height",
    "dN",
    "dE",
    "dU",
    "Sn",
    "Se",
    "Su",
    "Rne",
    "Rnu",
    "Reu",
    "Soln",
)


def classify_trace_file(path: Path) -> str:
    """Return 'network', 'station', or 'unknown'."""
    name = path.name
    if name.lower().startswith("network"):
        return "network"
    if _STATION_PREFIX_RE.match(name):
        return "station"
    return "unknown"


def find_trace_files(target: Path, recursive: bool = False) -> list[Path]:
    """Return TRACE files under the target path."""
    if target.is_file():
        return [target] if target.suffix.upper() == ".TRACE" else []
    direct = sorted(p for p in target.glob("*.TRACE") if p.is_file())
    if direct:
        return direct
    outputs_dir = target / "outputs"
    if outputs_dir.is_dir():
        hits = sorted(p for p in outputs_dir.glob("**/*.TRACE") if p.is_file())
        if hits:
            return hits
    if recursive:
        return sorted(p for p in target.rglob("*.TRACE") if p.is_file())
    return []


def find_pos_files(target: Path, recursive: bool = False) -> list[Path]:
    """Return POS files under the target path."""
    if target.is_file():
        return [target] if target.suffix.upper() == ".POS" else []
    direct = sorted(p for p in target.glob("*.POS") if p.is_file())
    if direct:
        return direct
    outputs_dir = target / "outputs"
    if outputs_dir.is_dir():
        hits = sorted(p for p in outputs_dir.glob("**/*.POS") if p.is_file())
        if hits:
            return hits
    if recursive:
        return sorted(p for p in target.rglob("*.POS") if p.is_file())
    return []


def parse_pos_file(path: Path) -> pd.DataFrame | None:
    """Parse a Ginan POS file into a DataFrame. Header metadata is stored in df.attrs."""
    rows: list[dict[str, Any]] = []
    metadata: dict[str, str | float] = {}

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()

            if line.startswith("PBO Station Position Time Series. Reference Frame :"):
                metadata["reference_frame"] = line.split(":")[-1].strip()
            elif line.startswith("4-character ID:"):
                metadata["station_id"] = line.split(":")[-1].strip()
            elif line.startswith("First Epoch"):
                metadata["first_epoch"] = line.split(":")[-1].strip()
            elif line.startswith("XYZ Reference position :"):
                parts = line.split(":")[-1].strip().split()
                if len(parts) >= 3:
                    try:
                        metadata["xyz_ref_x"] = float(parts[0])
                        metadata["xyz_ref_y"] = float(parts[1])
                        metadata["xyz_ref_z"] = float(parts[2])
                    except (ValueError, IndexError):
                        pass
            elif line.startswith("NEU Reference position :"):
                parts = line.split(":")[-1].strip().split()
                if len(parts) >= 3:
                    try:
                        metadata["neu_ref_lat"] = float(parts[0])
                        metadata["neu_ref_lon"] = float(parts[1])
                        metadata["neu_ref_height"] = float(parts[2])
                    except (ValueError, IndexError):
                        pass

            if not line or line.startswith(_POS_HEADER_PREFIXES):
                continue

            # Parse data lines (start with year)
            if line[0].isdigit():
                parts = line.split()
                if len(parts) >= 24:  # Ensure we have all expected columns
                    try:
                        rows.append(
                            {
                                "datetime": pd.to_datetime(parts[0]),
                                "decimal_year": float(parts[1]),
                                "X": float(parts[2]),
                                "Y": float(parts[3]),
                                "Z": float(parts[4]),
                                "Sx": float(parts[5]),
                                "Sy": float(parts[6]),
                                "Sz": float(parts[7]),
                                "Rxy": float(parts[8]),
                                "Rxz": float(parts[9]),
                                "Ryz": float(parts[10]),
                                "Nlat": float(parts[11]),
                                "Elong": float(parts[12]),
                                "Height": float(parts[13]),
                                "dN": float(parts[14]),
                                "dE": float(parts[15]),
                                "dU": float(parts[16]),
                                "Sn": float(parts[17]),
                                "Se": float(parts[18]),
                                "Su": float(parts[19]),
                                "Rne": float(parts[20]),
                                "Rnu": float(parts[21]),
                                "Reu": float(parts[22]),
                                "soln": parts[23] if len(parts) > 23 else None,
                            }
                        )
                    except (ValueError, IndexError):
                        continue

    if not rows:
        return None

    df = pd.DataFrame(rows)
    # pandas-stubs types .attrs as Mapping[Hashable | None, Any]; our metadata
    # dict (str keys only) is a valid value at runtime but not an exact match.
    df.attrs = metadata  # type: ignore[assignment]
    return df


@overload
def _reset(df: pd.DataFrame) -> pd.DataFrame: ...
@overload
def _reset(df: None) -> None: ...
def _reset(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None:
        return None
    return df.reset_index(drop=True)


def _select_residual_paths(path: Path) -> tuple[list[Path], bool]:
    """Return (trace_paths, should_emit) for residual parsing.

    When both a forward and _smoothed trace exist, the smoothed file drives
    parsing and should_emit=True; the forward file returns should_emit=False
    to avoid duplicate output.
    """
    suffix = path.suffix
    stem = path.stem

    if stem.endswith("_smoothed"):
        base = stem[:-9]
        forward = path.with_name(f"{base}{suffix}")
        paths: list[Path] = []
        if forward.exists():
            paths.append(forward)
        paths.append(path)
        return paths, True

    smoothed = path.with_name(f"{stem}_smoothed{suffix}")
    if smoothed.exists():
        return [path, smoothed], False

    return [path], True


def build_network_frames(path: Path, config: RunConfig) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    residual_paths, should_emit = _select_residual_paths(path)
    if should_emit:
        if config.keep_raw_residuals:
            residuals_df = trace.parse_residuals(
                residual_paths,
                strategy="both",
                forward_keep_last=False,
                include_source=True,
            )
            if residuals_df is not None:
                frames["network_residuals"] = _reset(residuals_df)
        else:
            residuals_df = trace.parse_residuals(
                residual_paths,
                strategy="smoothed",
                include_source=False,
            )
            if residuals_df is not None:
                frames["network_residuals_smoothed"] = _reset(residuals_df)

    # Smoothed traces don't contain large-error or ambiguity-reset entries
    is_smoothed = path.stem.endswith("_smoothed")
    if not is_smoothed:
        large_errors = trace.parse_large_errors(_line_iterator(path))
        frames["network_large_errors"] = _reset(large_errors)

        ambiguity_resets = trace.parse_ambiguity_resets(_line_iterator(path))
        frames["network_ambiguity_resets"] = _reset(ambiguity_resets)

    block_name = "STATES/PPP_RTS" if is_smoothed else "STATES/PPP"
    trop_df = trace.parse_trop_states(_line_iterator(path), block_name=block_name)
    if trop_df is not None and not trop_df.empty:
        suffix = "network_trop_smoothed" if is_smoothed else "network_trop"
        frames[suffix] = trop_df

    return frames


def build_station_frames(path: Path, config: RunConfig) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    pde_df = trace.parse_pde_cs(_line_iterator(path))
    if pde_df is not None:
        frames["station_pde_cs"] = _reset(pde_df)
        logger.info("  [parse_pde_cs] %d rows", len(pde_df))
    else:
        logger.info("  [parse_pde_cs] None")

    lc_df = trace.parse_lc(_line_iterator(path))
    if lc_df is not None:
        frames["station_lc"] = _reset(lc_df)
        logger.info("  [parse_lc] %d rows", len(lc_df))
    else:
        logger.info("  [parse_lc] None")

    detslp_df = trace.parse_detslp(_line_iterator(path))
    if detslp_df is not None:
        frames["station_detslp"] = _reset(detslp_df)
        logger.info("  [parse_detslp] %d rows", len(detslp_df))
    else:
        logger.info("  [parse_detslp] None")

    observations_df = trace.parse_observations(_line_iterator(path))
    if observations_df is not None:
        frames["station_observations"] = _reset(observations_df)
        logger.info("  [parse_observations] %d rows", len(observations_df))
    else:
        logger.info("  [parse_observations] None")

    return frames


def _engine(config: RunConfig) -> ParquetEngine:
    """config.engine comes straight from the CLI; pandas will raise its own
    error at runtime if the user passed something other than these three."""
    return cast(ParquetEngine, config.engine)


def write_frames(
    frames: dict[str, pd.DataFrame], base: str, config: RunConfig
) -> list[str]:
    written: list[str] = []
    for suffix, df in frames.items():
        if df is None:
            df = pd.DataFrame()
        out_path = config.output_dir / f"{base}_{suffix}.parquet"
        df.to_parquet(out_path, index=False, engine=_engine(config))
        row_count = len(df)
        status = "empty" if row_count == 0 else f"{row_count} rows"
        written.append(f"{suffix} ({status}) -> {out_path}")
    return written


def _line_iterator(path: Path) -> Iterator[str]:
    """Lazy line iterator that strips newlines without loading entire file."""
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line.rstrip("\r\n")


def process_trace_file(path: Path, config: RunConfig) -> list[str]:
    classification = classify_trace_file(path)
    frames: dict[str, pd.DataFrame] = {}

    if classification in ("network", "unknown"):
        frames.update(build_network_frames(path, config))
    if classification in ("station", "unknown"):
        frames.update(build_station_frames(path, config))

    if not frames:
        return [
            f"{path.name}: no data frames produced (classification={classification})"
        ]

    base = path.stem
    return write_frames(frames, base, config)


def process_pos_file(path: Path, config: RunConfig) -> list[str]:
    df = parse_pos_file(path)
    if df is None:
        return [f"{path.name}: no data parsed"]

    base = path.stem
    out_path = config.output_dir / f"{base}_pos.parquet"
    df.to_parquet(out_path, index=False, engine=_engine(config))
    row_count = len(df)
    status = "empty" if row_count == 0 else f"{row_count} rows"
    return [f"pos ({status}) -> {out_path}"]


def parse_args() -> Args:
    parser = argparse.ArgumentParser(
        description="Extract TRACE and POS DataFrames and write them to Parquet."
    )
    parser.add_argument(
        "target",
        type=Path,
        help="TRACE/POS file or directory containing TRACE/POS files (station date folder).",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        required=True,
        type=Path,
        help="Directory to write parquet files into.",
    )
    parser.add_argument(
        "--engine",
        help="Optional pandas parquet engine (e.g. pyarrow, fastparquet).",
        default="pyarrow",
    )
    parser.add_argument(
        "--keep-raw-residuals",
        action="store_true",
        help="Also persist the unfiltered residual observations alongside the final iteration.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively search for TRACE and POS files under the target directory.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-file output messages.",
    )
    namespace = parser.parse_args()
    return Args(**vars(namespace))


def main() -> None:
    args = parse_args()
    target = args.target.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Searching for TRACE/POS files in: %s", target)

    # Find both TRACE and POS files
    trace_files = find_trace_files(target, recursive=args.recursive)
    pos_files = find_pos_files(target, recursive=args.recursive)

    logger.info("Found %d TRACE files, %d POS files", len(trace_files), len(pos_files))

    if not trace_files and not pos_files:
        logger.error("No TRACE or POS files found under %s", target)
        sys.exit(1)

    config = RunConfig(
        output_dir=output_dir,
        engine=args.engine,
        keep_raw_residuals=args.keep_raw_residuals,
        quiet=args.quiet,
    )

    overall_written: list[str] = []

    for i, path in enumerate(trace_files, 1):
        logger.info("[%d/%d] TRACE: %s", i, len(trace_files), path.name)
        written = process_trace_file(path, config)
        overall_written.extend(written)
        if not config.quiet:
            for entry in written:
                logger.info("  %s", entry)

    for i, path in enumerate(pos_files, 1):
        logger.info("[%d/%d] POS: %s", i, len(pos_files), path.name)
        written = process_pos_file(path, config)
        overall_written.extend(written)
        if not config.quiet:
            for entry in written:
                logger.info("  %s", entry)

    if config.quiet:
        for entry in overall_written:
            logger.info(entry)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
