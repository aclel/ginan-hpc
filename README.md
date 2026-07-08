# ginan-hpc

PBS job scripts for running [Ginan](https://github.com/GeoscienceAustralia/ginan) PPP at scale on HPC clusters.

## What this does

Given a directory of downloaded RINEX observation files and IGS products, this toolset submits PBS array jobs that run Ginan's `pea` processor across many stations and dates in parallel. Each job patches the Ginan config with the correct product files, runs `pea`, converts the TRACE/POS outputs to Parquet, and cleans up scratch space.

It's been developed to work on single station PPP, not network mode. Optimisations have been made specifically for single station mode like using only one thread for each job, so to adapt for network mode will require consideration.

## Project structure

```
config/          Ginan config template and example station list
jobs/            PBS job templates (submit these with qsub)
scripts/         Python and shell scripts (called by PBS jobs and directly)
```

## Quickstart

### 1. Install Ginan

Install Ginan on the HPC. Follow instructions/guidelines for the specific HPC.

### 2. Set up the Python environment

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt  ## note that gnssanalysis uses Andrew Cleland fork until the TRACE file parsing is merged upstream
```

### 3. Download data

Edit and submit the download jobs:

```bash
# Download IGS products (orbits, clocks, biases, VMF3 grids)
qsub -v START=2024-01-01,END=2024-01-31,WORK_ROOT=/scratch/$USER/work jobs/download_products.pbs

# Download RINEX observations
qsub -v STATIONS=config/stations_example.txt,START=2024-01-01,END=2024-01-31,WORK_ROOT=/scratch/$USER/work jobs/download_rinex.pbs
```

### 5. Submit processing jobs

```bash
python scripts/submit_batch.py \
    --regenerate-manifest \
    --work-root $USER/work \
    --parquet-output-dir $USER/parquet \
    --config-file config/ppp_template.yaml \
    --mem 5000MB \
    --scratch-dir TMPDIR \
    --stations-file config/stations_example.txt \
    --submit-start-date 2019-01-01 \
    --submit-end-date 2019-01-31 \
```

## External dependencies

- **Ginan**: provides `pea` and `auto_download_PPP.py`. Tested with v4.1.1.
- **gnssanalysis**: Python library from Geoscience Australia, used for RINEX decompression and TRACE parsing (from Andrew Cleland fork until merged upstream).
- A **proxy** may be required for internet access from compute nodes. Set `PROXY_URL` before submitting download jobs if your cluster needs one.
