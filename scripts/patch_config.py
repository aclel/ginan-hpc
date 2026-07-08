#!/usr/bin/env python3
"""Patch Ginan config files with day-specific product paths.

Can be imported (discover_product_files, patch_config_with_products) or
run as a script in two modes:
  range   Copy and patch configs for every date in a date range
  single  Patch a single config file in place
"""

import argparse
import logging
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, TypedDict, cast

import yaml


class ProductFiles(TypedDict):
    nav_files: list[str]
    clk_files: list[str]
    bsx_files: list[str]
    sp3_files: list[str]
    snx_files: list[str]
    vmf_files: list[str]


@dataclass(frozen=True)
class RangeArgs:
    config: Path
    start: str
    end: str
    work_root: Path
    verbose: bool


@dataclass(frozen=True)
class SingleArgs:
    config: Path
    products_dir: Path
    output_dir: Path | None
    verbose: bool


def daterange(start_date: str, end_date: str) -> Iterator[str]:
    """Yield YYYY-MM-DD strings from start to end (inclusive)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def discover_product_files(products_dir: Path) -> ProductFiles:
    """Scan products_dir and return filenames grouped by product type."""
    product_files: ProductFiles = {
        "nav_files": [],
        "clk_files": [],
        "bsx_files": [],
        "sp3_files": [],
        "snx_files": [],
        "vmf_files": [],
    }

    for f in products_dir.glob("*"):
        if not f.is_file():
            continue
        suffix_lower = f.suffix.lower()

        if suffix_lower == ".rnx":
            product_files["nav_files"].append(f.name)
        elif suffix_lower == ".clk":
            product_files["clk_files"].append(f.name)
        elif suffix_lower == ".bia":
            product_files["bsx_files"].append(f.name)
        elif suffix_lower == ".sp3":
            product_files["sp3_files"].append(f.name)
        elif suffix_lower == ".snx":
            if (
                f.name != "igs_satellite_metadata.snx"
            ):  # static file lives in the template
                product_files["snx_files"].append(f.name)
        elif f.name.startswith("VMF3_"):
            product_files["vmf_files"].append(f.name)

    # TypedDict values can't be indexed by a dynamic key under strict mypy even
    # though every field here is list[str]; .values() sidesteps the dynamic-key
    # restriction while cast() documents the (already-true) uniform value type.
    for files in product_files.values():
        cast(list[str], files).sort()

    return product_files


def patch_config_with_products(config_path: Path, product_files: ProductFiles) -> None:
    """Rewrite config_path with actual product filenames for the day."""
    with open(config_path, "r") as f:
        cfg: dict[str, Any] = yaml.safe_load(f)

    if "inputs" not in cfg:
        cfg["inputs"] = {}
    if "satellite_data" not in cfg["inputs"]:
        cfg["inputs"]["satellite_data"] = {}

    if product_files["nav_files"]:
        cfg["inputs"]["satellite_data"]["nav_files"] = product_files["nav_files"]

    if product_files["clk_files"]:
        cfg["inputs"]["satellite_data"]["clk_files"] = product_files["clk_files"]

    if product_files["bsx_files"]:
        cfg["inputs"]["satellite_data"]["bsx_files"] = product_files["bsx_files"]

    if product_files["sp3_files"]:
        cfg["inputs"]["satellite_data"]["sp3_files"] = product_files["sp3_files"]

    if product_files["snx_files"]:
        existing_snx: list[str] = cfg["inputs"].get(
            "snx_files",
            [
                "igs_satellite_metadata.snx",
                "tables/sat_yaw_bias_rate.snx",
                "tables/qzss_yaw_modes.snx",
                "tables/bds_yaw_modes.snx",
                "IGc20.ssc",
            ],
        )

        for snx_file in product_files["snx_files"]:
            if snx_file not in existing_snx:
                existing_snx.append(snx_file)

        cfg["inputs"]["snx_files"] = existing_snx

    if product_files["vmf_files"]:
        if "troposphere" not in cfg["inputs"]:
            cfg["inputs"]["troposphere"] = {}
        cfg["inputs"]["troposphere"]["vmf_files"] = sorted(product_files["vmf_files"])

    with open(config_path, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)

    for prod_type, files in product_files.items():
        if files:
            logging.info("  %s: %s", prod_type, ", ".join(cast(list[str], files)))


def patch_config_for_date(
    date: str, work_root: Path, source_config: Path, config_name: str
) -> bool:
    """Copy source_config into work_root/date/ and patch it with that day's products."""
    try:
        work_dir = work_root / date
        products_dir = work_dir / "products"

        if not work_dir.exists():
            logging.warning("%s: work directory not found, skipping", date)
            return False
        if not products_dir.exists():
            logging.warning("%s: products/ not found, skipping", date)
            return False

        dest_config = work_dir / config_name
        shutil.copy2(source_config, dest_config)
        patch_config_with_products(dest_config, discover_product_files(products_dir))
        return True

    except Exception as e:
        logging.error("%s: %s", date, e)
        return False


def parse_args() -> RangeArgs | SingleArgs:
    parser = argparse.ArgumentParser(
        description="Copy and patch Ginan config files with day-specific product paths"
    )
    subparsers = parser.add_subparsers(dest="command", help="Patch mode")

    range_parser = subparsers.add_parser("range", help="Patch configs for a date range")
    range_parser.add_argument(
        "--config", type=Path, required=True, help="Config file to copy and patch"
    )
    range_parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    range_parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    range_parser.add_argument(
        "--work-root", type=Path, required=True, help="Base work directory"
    )

    single_parser = subparsers.add_parser(
        "single", help="Patch a single config file in place"
    )
    single_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Config file to patch (modified in place)",
    )
    single_parser.add_argument(
        "--products-dir",
        type=Path,
        required=True,
        help="Directory containing product files",
    )
    single_parser.add_argument(
        "--output-dir", type=Path, help="Set outputs_root in config to this directory"
    )

    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    namespace = parser.parse_args()

    if not namespace.command:
        parser.print_help()
        raise SystemExit(1)

    command = namespace.command
    del namespace.command
    if command == "range":
        return RangeArgs(**vars(namespace))
    return SingleArgs(**vars(namespace))


def run_range(args: RangeArgs) -> int:
    if not args.config.exists():
        logging.error("Config file not found: %s", args.config)
        return 1
    if not args.work_root.exists():
        logging.error("Work root not found: %s", args.work_root)
        return 1
    try:
        datetime.strptime(args.start, "%Y-%m-%d")
        datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid date format, use YYYY-MM-DD")
        return 1

    dates = list(daterange(args.start, args.end))
    logging.info("Patching %d dates (%s to %s)", len(dates), args.start, args.end)

    n_ok = sum(
        patch_config_for_date(d, args.work_root, args.config, args.config.name)
        for d in dates
    )
    n_fail = len(dates) - n_ok
    logging.info("%d/%d dates patched successfully", n_ok, len(dates))
    if n_fail:
        logging.warning("%d dates failed", n_fail)
    return 0 if n_fail == 0 else 1


def run_single(args: SingleArgs) -> int:
    if not args.config.exists():
        logging.error("Config file not found: %s", args.config)
        return 1
    if not args.products_dir.exists():
        logging.error("Products directory not found: %s", args.products_dir)
        return 1

    try:
        patch_config_with_products(
            args.config, discover_product_files(args.products_dir)
        )

        if args.output_dir:
            with open(args.config, "r") as f:
                cfg: dict[str, Any] = yaml.safe_load(f)
            if "outputs" not in cfg:
                cfg["outputs"] = {}
            cfg["outputs"]["outputs_root"] = f"{args.output_dir}/outputs/<CONFIG>"
            with open(args.config, "w") as f:
                yaml.safe_dump(cfg, f, sort_keys=False)

        logging.info("Patched: %s", args.config)
        return 0

    except Exception as e:
        logging.error("Error patching config: %s", e)
        return 1


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if isinstance(args, RangeArgs):
        return run_range(args)
    return run_single(args)


if __name__ == "__main__":
    exit(main())
