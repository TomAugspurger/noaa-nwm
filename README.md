# NWM on Azure

Materials for the Azure / Planetary Computer workshop at the [CIROH Training and Developers Conference][conf].

## Log into the Hub

Log into the JupyterHub at <https://ciroh.eastus.cloudapp.azure.com/>.

## Notebooks

These notebooks give an overview of national water model data on Azure, and an introduction to cloud-native geospatial data analysis.

1. [Introduction](noaa-nwm-example.ipynb): An overview of the NWM data available on Azure.
2. [Problems](Problems.ipynb): Some challenges with using NWM data on the cloud.
3. [Using Kerchunk](using-kerchunk.ipynb): Using Kerchunk to speed up access to NWM data.
4. [Tabular](nwm-tabular.ipynb): Accessing NWM data as a geoparquet dataset.

## Data Processing

The `processing` directory has some scripts for

1. Creating Kerchunk index files
2. Rechunking the data
3. Converting the `reservoir` data to a `geoparquet` dataset

Check those out if you're interested in some of the behind-the-scenes work for making data available on the cloud.

## Infrastructure

The `terraform` directory has some stuff for deploying the infrastructure in the workshop.

[conf]: https://ciroh.ua.edu/devconference/