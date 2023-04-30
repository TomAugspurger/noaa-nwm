import os
import pathlib

import fsspec
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from dask_kubernetes.operator import KubeCluster, make_cluster_spec
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
    recipe = XarrayZarrRecipe(
        pattern,
        cache_inputs=False,
    )
    # configure storage
    credential = os.environ["AZURE_SAS_TOKEN"]
    product = "forcing"
    target_storage_options = dict(account_name="noaanwm", credential=credential)
    target_fs = fsspec.filesystem("abfs", **target_storage_options)
    storage = StorageConfig(
        target=MyTarget(target_fs, root_path=f"ciroh/zarr/short-range-{product}.zarr/"),
        metadata=MyMetadataTarget(
            target_fs, root_path=f"ciroh/metadata/short-range-{product}-zarr-metadata/"
        ),
    )
    recipe.storage_config = storage
    spec = make_cluster_spec(
        name="nwm",
        n_workers=64,
        image="pccomponentstest.azurecr.io/noaa-nwm:2023.4.26.0",
        resources={
            "requests": {"memory": "7Gi", "cpu": "0.9"},
            "limit": {"memory": "8Gi", "cpu": "1"},
        },
        worker_command="dask-worker --nthreads 1 --nworkers 1 --memory-limit 8GB",
    )
    spec["spec"]["worker"]["spec"]["tolerations"] = [
        {
            "key": "k8s.dask.org/dedicated",
            "operator": "Equal",
            "value": "worker",
            "effect": "NoSchedule",
        },
        {
            "key": "k8s.dask.org_dedicated",
            "operator": "Equal",
            "value": "worker",
            "effect": "NoSchedule",
        },
        {
            "key": "kubernetes.azure.com/scalesetpriority",
            "operator": "Equal",
            "value": "spot",
            "effect": "NoSchedule",
        },
    ]

    with KubeCluster(custom_cluster_spec=spec) as cluster:
        # cluster.scale(64)
        with cluster.get_client() as client:
            client.upload_file("run_zarr.py")
            print("Dashboard Link:", client.dashboard_link)
            recipe.to_dask().compute()


if __name__ == "__main__":
    main()
