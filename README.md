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

## Setup

### 1. Install Ginan

Install Ginan on the HPC. Follow instructions/guidelines for the specific HPC.

### 2. Set up the Python environment

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt  ## note that gnssanalysis uses Andrew Cleland fork until the TRACE file parsing is merged upstream
```

### 3. Source the product files you need

Everyone may choose different products so I'll leave it up to you to get those into the working directory.

The working directory contains all of the products and rinex for each date, and will eventually contain log files for all of the pea jobs that run.

```
$HOME/work/2019-01-01/products --this contains all of the required input products (orbits, clocks, biases, VMF3 grids, static files symlinks)
```

You'll need to store the "static" products somewhere on HPC. I store them in home directory, and then in the dated folders I symlink back to these.

```
~/products_template/:
finals.data.iau2000.txt  IGc20.ssc  igs20.atx  igs_satellite_metadata.snx  psd_IGc20.snx  tables

~/products_template/tables:
ALOAD_GO.BLQ  bds_yaw_modes.snx  DE436.1950.2050  gpt_25.grd  igrf13coeffs.txt  igrf14coeffs.txt  OLOAD_GO.BLQ  opoleloadcoefcmcor.txt  orography_ell_1x1.txt  qzss_yaw_modes.snx  sat_yaw_bias_rate.snx
```

### 3. Download RINEX files

Create ~/.netrc with Earthdata credentials for NASA CDDIS access:

```
machine urs.earthdata.nasa.gov login YOUR_USERNAME password YOUR_PASSWORD
```

Submit download jobs (calls `auto_download_PPP.py` in the background):

```bash
# Download RINEX observation files to work dir
qsub -v START_DATE=2019-01-01,END_DATE=2019-01-31,STATIONS=config/stations_example.txt,WORK_ROOT=$HOME/work,GINAN_ROOT=$HOME/projects/ginan -l walltime=1:00:00 jobs/download_rinex.pbs
```

### 4. Submit pea jobs

```bash
python scripts/submit_batch.py \
    --regenerate-manifest \
    --work-root $HOME/work \
    --parquet-output-dir $HOME/parquet \
    --config-file config/ppp_template.yaml \
    --mem 5000MB \
    --scratch-dir TMPDIR \
    --stations-file config/stations_example.txt \
    --submit-start-date 2019-01-01 \
    --submit-end-date 2019-01-31
```

Check for the pea logs in workdir/2019-01-01/logs/ALIC.log for example.

Check for output parquet files in output directory.

## PBS commands

Check current jobs with `qstat -a`

To check all array jobs `qstat -Jt`

Get info about a job after it's finished running `qstat -xf <job>`

Delete a running job with `qdel <job>`

## Pea workload

When parallelised the pea is heavy on disk IO. Running in parallel saturates the disk. HPCs normally have fast scratch space available on the compute nodes. The pbs scripts are set up to write TRACE files and prepare outputs on this scratch disk and then write back to output dir when it's finished.

During testing I also tried tmpfs (RAM backed filesystem). This may also be an option depending on whether the HPC you're using supports it.

## External dependencies

- **Ginan**: provides `pea` and `auto_download_PPP.py`. Tested with v4.1.1.
- **gnssanalysis**: Python library from Geoscience Australia, used for RINEX decompression and TRACE parsing (from Andrew Cleland fork until merged upstream).
- A **proxy** may be required for internet access from compute nodes. Set `PROXY_URL` before submitting download jobs if your cluster needs one.
