import os
import sys
import pathlib
import argparse
from typing import Any
import typing

from dask_kubernetes.operator import KubeCluster
import azure.storage.blob
import dask.dataframe
import datetime
import fsspec
import geopandas
import kerchunk.combine
import kerchunk.hdf
import numpy as np
import pandas as pd
import pyproj
import tlz
import xarray as xr

__version__ = "0.1.0"

STORAGE_OPTIONS = dict(account_name="noaanwm")
CYCLE_RUNTIMES = list(range(23))


def make_prefix(date, product, cycle_runtime):
    return (
        f"https://noaanwm.blob.core.windows.net/nwm/"
        f"nwm.{date:%Y%m%d}/{product}/nwm.t{cycle_runtime:0>2d}z.{product}"
    )


def make_url(date, product, cycle_runtime, kind, forecast_time):
    """
    >>> channel_rt = xr.open_dataset(
    ...     fsspec.open(make_url(date, "short_range", 0, "channel_rt", 1)).open()
    ... )
    """
    prefix = make_prefix(date, product, cycle_runtime)
    return ".".join([prefix, kind, f"f{forecast_time:0>3d}", "conus.nc"])


def list_date(
    protocol: str, storage_options: dict[str, Any], date: datetime.date, product: str
) -> list[str]:
    fs = fsspec.filesystem(protocol, **storage_options)
    paths = fs.glob(
        f"nwm/nwm.{date:%Y%m%d}/{product}/nwm.t00z.{product}.channel_rt.f*.conus.nc"
    )
    paths = [f"{fs.protocol}://{p}" for p in paths]
    return paths


def list_day(date: datetime.date, kind):
    cc = azure.storage.blob.ContainerClient(
        "https://noaanwm.blob.core.windows.net", "nwm"
    )
    names = list(cc.list_blob_names(name_starts_with=f"nwm.{date:%Y%m%d}/short_range"))
    names = [x for x in names if x.endswith(f"{kind}.f001.conus.nc")]
    urls = [f"https://noaanwm.blob.core.windows.net/nwm/{name}" for name in names]
    return urls


def generate(
    protocol: str,
    storage_options: dict[str, Any],
    dates: datetime.date | typing.Sequence[datetime.date],
    product: str,
) -> dict:
    if isinstance(dates, datetime.date):
        dates = [dates]

    list_dates_ = dask.delayed(list_date)
    single = dask.delayed(kerchunk.hdf.SingleHdf5ToZarr)

    print("listing files")
    files = list(
        tlz.concat(
            dask.compute(
                *[
                    list_dates_(protocol, storage_options, date, product)
                    for date in dates
                ]
            )
        )
    )

    print("generating indices")
    indices = dask.compute(
        *[single(f, storage_options=storage_options).translate() for f in files]
    )

    print("merging indices")
    d = kerchunk.combine.MultiZarrToZarr(
        indices,
        remote_protocol=protocol,
        concat_dims=["time", "reference_time"],
        remote_options=storage_options,
    ).translate()

    return d


def to_dataframe(ds):
    crs = pyproj.CRS.from_epsg(4326)
    geometry = geopandas.points_from_xy(ds.longitude, ds.latitude, crs=crs)

    df = geopandas.GeoDataFrame(
        {
            "reservoir_type": pd.Categorical(ds.reservoir_type.data, categories=[1, 2]),
            "water_sfc_elev": ds.water_sfc_elev,
            "inflow": ds.inflow,
            "outflow": ds.outflow,
        },
        geometry=geometry,
    )

    df["time"] = np.repeat(ds.time.data, len(df))
    df["feature_id"] = ds.feature_id

    df = df[
        [
            "time",
            "feature_id",
            "geometry",
            "reservoir_type",
            "water_sfc_elev",
            "inflow",
            "outflow",
        ]
    ]
    return df


def process_day(url):
    ds = xr.open_dataset(fsspec.open(url).open(), engine="h5netcdf").load()
    return to_dataframe(ds)


def process_month(urls):
    dfs = [process_day(url) for url in urls]
    df = pd.concat(dfs).set_index("time")
    return df


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", default="ciroh/short-range-reservoir.parquet")

    return parser.parse_args()


def main(args=None):
    args = parse_args(args)
    prefix = args.prefix

    credential = os.environ["AZURE_SAS_TOKEN"]

    p = pathlib.Path("urls.txt")
    if not p.exists():
        print("Listing files")

        cc = azure.storage.blob.ContainerClient(
            "https://noaanwm.blob.core.windows.net", "nwm"
        )
        roots = [x.name for x in cc.walk_blobs("nwm")]
        dates = [pd.Timestamp(root[4:-1]) for root in roots]
        with dask.distributed.Client() as client:
            print("Dashboard Link", client.dashboard_link)
            urls = dask.compute(
                *[dask.delayed(list_day)(date, "reservoir") for date in dates]
            )
        with open("urls.txt", "w") as f:
            f.write("\n".join(tlz.concat(list(urls))))

    def month_key(x):
        return x.split("/")[4].split(".")[1][:6]

    urls = sorted(open("urls.txt").read().split())
    by_month = list(tlz.partitionby(month_key, urls))

    months = []
    for chunk in by_month:
        x = chunk[0]
        months.append(datetime.datetime.strptime(month_key(x), "%Y%m"))

    storage_options = {
        "account_name": "noaanwm",
        "credential": credential,
    }

    schema = geopandas.GeoDataFrame(
        {
            "feature_id": np.array([], dtype="int32"),
            "geometry": geopandas.array.GeometryArray(np.array([]), crs="epsg:4326"),
            "reservoir_type": pd.Categorical([], categories=[1, 2]),
            "water_sfc_elev": np.array([], dtype="float32"),
            "inflow": np.array([], dtype="float64"),
            "outflow": np.array([], dtype="float64"),
        },
        index=pd.DatetimeIndex([], dtype="datetime64[ns]", name="time", freq=None),
    )

    last = months[-1] + pd.tseries.offsets.MonthEnd() + pd.tseries.offsets.Hour(n=24)
    divisions = tuple([month + pd.tseries.offsets.Hour() for month in months]) + (last,)

    df = dask.dataframe.from_map(
        process_month, by_month, meta=schema, divisions=divisions
    )

    with KubeCluster(
        image="mcr.microsoft.com/planetary-computer/python:2023.3.19.0",
        resources={
            "requests": {"memory": "7Gi", "cpu": "0.9"},
            "limit": {"memory": "8Gi", "cpu": "1"},
        },
        worker_command="dask-worker --nthreads 1 --nworkers 1 --memory-limit 8GB",
    ) as cluster:
        cluster.scale(8)
        with cluster.get_client() as client:
            print("Dashboard Link:", client.dashboard_link)
            df.to_parquet(
                f"abfs://{prefix}",
                write_metadata_file=True,
                storage_options=storage_options,
            )


if __name__ == "__main__":
    sys.exit(main())
