# Data Processing

For this workshop, we use both the raw data pushed to Azure by NODD and some transformations of that data.
The code for these transformations is in this directory.

All processing occurs on the AKS cluster we set up for the workshop.

## Kerchunk Index Files

The `run_kerchunk.py` file uses `pangeo-forge-recipes` to create an index file for a product (channel_rt, land, forcing). This *should* be a pretty straightforward process, but we had to work around a few issues

1. https://github.com/pangeo-forge/pangeo-forge-recipes/issues/515 requires updating the `scan_file` used to pass through the inline threshold for Kerchunk
2. https://github.com/pangeo-forge/pangeo-forge-recipes/issues/515, paired with using SAS tokens for authentication to Blob Storage, requires patching up the storage classes slightly
3. There were a few corrupt NetCDF files in the archive. I reported these to NOAA and they've since been fixed, but I haven't regenerated the index file.
4. In June 2022, the internal chunking of the National Water Model files changed. Kerchunk currently requires identical chunking across files, so this is limited to just the newer files.

## Zarr conversion

The `run_zarr.py` module creates Zarr copy of the `forcing` data using pangeo-forge-recipes. This was pretty straightforward, aside from some special code to ensure that the datetimes are encoded properly.

## Rechunking

The `rechunk_nwm.py` module rechunks the Zarr dataset to work well for timeseries analysis. I would typically use [rechunker](https://rechunker.readthedocs.io/) for this, but I wanted to test out Dask's new [rechunking / shuffle service](https://discourse.pangeo.io/t/rechunking-large-data-at-constant-memory-in-dask-experimental/3266). There are some rough edges (like getting `to_zarr` to work), but overall the performance and stability was great.