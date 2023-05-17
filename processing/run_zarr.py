import os
import azure.storage.blob
import datetime
import pandas as pd
import zarr
import pathlib

import fsspec
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from dask_kubernetes.operator import KubeCluster
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, MetadataTarget

BAD = {
    "nwm/nwm.20220917/short_range/nwm.t18z.short_range.channel_rt.f001.conus.nc",
    "nwm/nwm.20220926/short_range/nwm.t16z.short_range.channel_rt.f001.conus.nc",
    "nwm/nwm.20220913/short_range/nwm.t12z.short_range.land.f001.conus.nc",
    "nwm/nwm.20220927/short_range/nwm.t20z.short_range.land.f001.conus.nc",
    "nwm/nwm.20221020/forcing_short_range/nwm.t00z.short_range.forcing.f001.conus.nc",
}


# Workaround https://github.com/pangeo-forge/pangeo-forge-recipes/issues/419
class MyTarget(FSSpecTarget):
    def __post_init__(self):
        pass


# Workaround https://github.com/pangeo-forge/pangeo-forge-recipes/issues/419
class MyMetadataTarget(MetadataTarget):
    def __post_init__(self):
        pass


def process_input(ds: xr.Dataset, filename: str) -> xr.Dataset:
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/318
    # Ensure that the timestamps are correct in the output
    ds["time"].encoding["units"] = "minutes since 1970-01-01 00:00:00 UTC"
    return ds


def main():
    file_list = pathlib.Path("forcing-files.txt").read_text().splitlines()
    file_list = [
        x
        for x in file_list
        # https://github.com/pangeo-forge/staged-recipes/pull/215/#issuecomment-1520905668
        # filter to newer files
        if x.split("/")[1].split(".")[1] > "20220628"
        # drop corrupt NetCDF files
        and x not in BAD
    ]
    print(f"Processing {len(file_list)} files")

    # fs = fsspec.filesystem("abfs", account_name="noaanwm")
    urls = ["abfs://" + f for f in file_list]

    pattern = pattern_from_file_sequence(
        urls, "time", nitems_per_file=1, fsspec_open_kwargs=dict(account_name="noaanwm")
    )
    recipe = XarrayZarrRecipe(pattern, cache_inputs=False, process_input=process_input)
    # configure storage
    credential = os.environ["AZURE_SAS_TOKEN"]
    product = "forcing"
    target_storage_options = dict(account_name="noaanwm", credential=credential)
    target_fs = fsspec.filesystem("abfs", **target_storage_options)
    storage = StorageConfig(
        target=MyTarget(
            target_fs, root_path=f"ciroh/zarr/ts/short-range-{product}-test.zarr/"
        ),
        metadata=MyMetadataTarget(
            target_fs, root_path=f"ciroh/metadata/short-range-{product}-zarr-metadata/"
        ),
    )
    recipe.storage_config = storage

    with KubeCluster(custom_cluster_spec="cluster.yaml") as cluster:
        with cluster.get_client() as client:
            client.upload_file("run_zarr.py")
            print("Dashboard Link:", client.dashboard_link)
            recipe.to_dask().compute(retries=10)
            print("Done")

    fix_time(urls, credential)


def fix_time(urls, credential):
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/318
    dates = []
    for url in urls:
        ymd = url[15:23]
        hh = url[49:51]
        dates.append(datetime.datetime.strptime(f"{ymd}{hh}", "%Y%m%d%H"))

    dates = pd.to_datetime(dates)
    time = xr.DataArray(
        dates,
        dims=["time"],
        name="time",
        attrs={"long_name": "valid output time", "standard_name": "time"},
    )
    store = {}
    time.to_zarr(store)
    cc = azure.storage.blob.ContainerClient(
        "https://noaanwm.blob.core.windows.net",
        container_name="ciroh",
        credential=credential,
    )
    prefix = "zarr/ts/short-range-forcing-test.zarr"

    for k, v in store.items():
        if k.startswith("time/"):
            print("fix", k)
            cc.get_blob_client(f"{prefix}/{k}").upload_blob(v, overwrite=True)

    print("Reconsolidating metadata")
    rstore = zarr.ABSStore(client=cc, prefix=prefix)
    zarr.consolidate_metadata(rstore)


if __name__ == "__main__":
    main()
