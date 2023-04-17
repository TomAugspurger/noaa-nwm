"""
Stuff for NOAA NWM

https://planetarycomputer.microsoft.com/dataset/storage/noaa-nwm

The 'reservoir' data is tabular-ish?

    https://noaanwm.blob.core.windows.net/nwm/nwm.20230401/short_range/nwm.t00z.short_range.reservoir.f001.conus.nc

```
In [107]: x = xr.open_dataset(fsspec.open("https://noaanwm.blob.core.windows.net/nwm/nwm.20230401/short_range/nwm.t00z.short_range.reservoir.f001.conus.nc").open())
In [108]: df = geopandas.GeoDataFrame(x.drop_vars(["crs", "latitude", "longitude"]).to_dataframe(), geometry=geopandas.points_from_xy(x.longitude, x.latitude))
```


"""
from typing import Any
import typing
import fsspec
import kerchunk.hdf
import kerchunk.combine

import adlfs
import kerchunk.hdf
import kerchunk.combine
import adlfs
import fsspec
import datetime
import xarray as xr
import dask
import pandas as pd
import tlz
import enum


STORAGE_OPTIONS = dict(account_name="noaanwm")

# fs = adlfs.AzureBlobFileSystem("noaanwm")
# storage_options = {"account_name": "noaanwm"}

# protocol = "abfs"
# product = "short_range"
# start_date = datetime.date(2022, 9, 11)
# end_date = datetime.date(2022, 9, 14)


def make_prefix(date, product, cycle_runtime):
    return f"https://noaanwm.blob.core.windows.net/nwm/nwm.{date:%Y%m%d}/{product}/nwm.t{cycle_runtime:0>2d}z.{product}"



def make_url(date, product, cycle_runtime, kind, forecast_time):
    """

    >>> channel_rt = xr.open_dataset(fsspec.open(make_url(date, "short_range", 0, "channel_rt", 1)).open())
    """
    prefix = make_prefix(date, product, cycle_runtime)
    return ".".join([prefix, kind, f"f{forecast_time:0>3d}", "conus.nc"])


def list_date(protocol: str, storage_options: dict[str, Any], date: datetime.date, product: str) -> list[str]:
    fs = fsspec.filesystem(protocol, **storage_options)
    paths = fs.glob(f"nwm/nwm.{date:%Y%m%d}/{product}/nwm.t00z.{product}.channel_rt.f*.conus.nc")
    paths = [f"{fs.protocol}://{p}" for p in paths]
    return paths


def generate(protocol: str, storage_options: dict[str, Any], dates: datetime.date | typing.Sequence[datetime.date], product: str) -> dict:
    if isinstance(dates, datetime.date):
        dates = [dates]

    list_dates_ = dask.delayed(list_date)
    single = dask.delayed(kerchunk.hdf.SingleHdf5ToZarr)

    print("listing files")
    files = list(tlz.concat(dask.compute(*[list_dates_(protocol, storage_options, date, product) for date in dates])))

    print("generating indices")
    indices = dask.compute(*[single(f, storage_options=storage_options).translate() for f in files])

    print("merging indices")
    d = kerchunk.combine.MultiZarrToZarr(indices, remote_protocol=protocol, concat_dims=['time', 'reference_time'], remote_options=storage_options).translate()
    
    return d
