#!/bin/bash
# Run pea on a single (date, station) pair, convert outputs to Parquet, and clean up.
#
# 1. Decompresses .crx.gz to .rnx via gnssanalysis if needed (keeps original)
# 2. Patches config with day-specific product filenames
# 3. Runs pea with outputs directed to SCRATCH_DIR
# 4. Converts TRACE/POS to Parquet and copies to PARQUET_OUTPUT_DIR
# 5. Cleans up scratch
#
# Usage: run_ginan.sh DATE STATION WORK_ROOT PARQUET_OUTPUT_DIR REPO_ROOT CONFIG_FILE SCRATCH_DIR
#
# Environment:
#   OMP_NUM_THREADS  Number of threads for pea (default: 1)

set -e

DATE="${1:?DATE not provided}"
STATION="${2:?STATION not provided}"
WORK_ROOT="${3:?WORK_ROOT not provided}"
PARQUET_OUTPUT_DIR="${4:?PARQUET_OUTPUT_DIR not provided}"
REPO_ROOT="${5:?REPO_ROOT not provided}"
CONFIG_FILE="${6:?CONFIG_FILE not provided}"
SCRATCH_DIR="${7:?SCRATCH_DIR not provided}"

WORK_DIR="${WORK_ROOT}/${DATE}"
DATA_DIR="${WORK_DIR}/data"

RINEX_FILE=$(find "${DATA_DIR}" -name "${STATION}*.rnx" -type f | head -1)

if [ -z "${RINEX_FILE}" ]; then
    CRX_FILE=$(find "${DATA_DIR}" -name "${STATION}*.crx.gz" -o -name "${STATION}*.crx" | head -1)
    if [ -z "${CRX_FILE}" ]; then
        echo "ERROR: no RINEX or CRX file for ${STATION} in ${DATA_DIR}" >&2
        exit 1
    fi

    echo "Decompressing: $(basename "${CRX_FILE}")"
    if ! python3 -c "
from pathlib import Path
from gnssanalysis.gn_download import decompress_file
decompress_file(Path('${CRX_FILE}'))
"; then
        echo "ERROR: decompression failed: ${CRX_FILE}" >&2
        exit 1
    fi

    RINEX_FILE=$(find "${DATA_DIR}" -name "${STATION}*.rnx" -type f | head -1)
    if [ -z "${RINEX_FILE}" ]; then
        echo "ERROR: .rnx not found after decompression" >&2
        exit 1
    fi
fi

RINEX_NAME=$(basename "${RINEX_FILE}")
DATASET="${RINEX_NAME%.*}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"

JOB_SCRATCH="${SCRATCH_DIR}/ginan_${DATE}_${STATION}"
mkdir -p "${JOB_SCRATCH}"

CONFIG_NAME=$(basename "${CONFIG_FILE}")
SCRATCH_CONFIG="${JOB_SCRATCH}/${CONFIG_NAME}"
cp "${CONFIG_FILE}" "${SCRATCH_CONFIG}"

echo "Patching config for ${DATE}..."
if ! python3 "${REPO_ROOT}/scripts/patch_config.py" single \
    --config "${SCRATCH_CONFIG}" \
    --products-dir "${WORK_DIR}/products" \
    --output-dir "${JOB_SCRATCH}"; then
    echo "ERROR: config patching failed for ${STATION}" >&2
    exit 1
fi

# Run pea from WORK_DIR so relative paths to products/ and data/ resolve correctly
cd "${WORK_DIR}"
if ! pea --config "${SCRATCH_CONFIG}" -r "${RINEX_NAME}" -d "${DATASET}"; then
    echo "ERROR: pea failed for ${STATION}" >&2
    exit 1
fi

GINAN_OUTPUT_DIR="${JOB_SCRATCH}/outputs/${DATASET}"
if [ ! -d "${GINAN_OUTPUT_DIR}" ]; then
    echo "WARNING: no output directory for ${STATION} at ${GINAN_OUTPUT_DIR} — skipping Parquet"
    exit 0
fi

PARQUET_SCRATCH_DIR="${SCRATCH_DIR}/parquet"
mkdir -p "${PARQUET_SCRATCH_DIR}"

if ! python3 -u "${REPO_ROOT}/scripts/save_outputs_parquet.py" \
    "${GINAN_OUTPUT_DIR}" \
    --output-dir "${PARQUET_SCRATCH_DIR}"; then
    echo "ERROR: Parquet conversion failed for ${STATION}" >&2
    exit 1
fi

PARQUET_STATION_DIR="${PARQUET_OUTPUT_DIR}/${DATE}/${STATION}"
mkdir -p "${PARQUET_STATION_DIR}"
cp -r "${PARQUET_SCRATCH_DIR}"/* "${PARQUET_STATION_DIR}/"

rm -rf "${JOB_SCRATCH}"

# Remove decompressed .rnx only if the original .crx.gz is still present
CRX_ORIGINAL=$(find "${DATA_DIR}" -name "${STATION}*.crx.gz" -o -name "${STATION}*.crx" | head -1)
if [ -n "${CRX_ORIGINAL}" ] && [ -f "${RINEX_FILE}" ]; then
    rm -f "${RINEX_FILE}"
fi

echo "Done: ${STATION} ${DATE}"
