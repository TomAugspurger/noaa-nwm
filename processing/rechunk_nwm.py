import os
import sys
import argparse
import rechunker
import xarray as xr
import fsspec
from dask_kubernetes.operator import KubeCluster, make_cluster_spec


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    # parser.add_argument("product", choices=["channel_rt", "land"])

    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)
    # product = args.product
    product = "forcing"

    credential = os.environ["AZURE_SAS_TOKEN"]

    ds = xr.open_dataset(
        "abfs://ciroh/zarr/short-range-forcing.zarr",
        engine="zarr",
        consolidated=True,
        chunks={},
        storage_options={"account_name": "noaanwm"},
    )

    target_chunks = {
        "time": 2000,
        "x": 100,
        "y": 100,
    }
    target_store = fsspec.get_mapper(
        "abfs://ciroh/zarr/short-range-forcing-rechunked.zarr",
        account_name="noaanwm",
        credential=credential,
    )
    temp_store = fsspec.get_mapper(
        f"abfs://ciroh/rechunk/{product}/temp",
        account_name="noaanwm",
        credential=credential,
    )
    target_store.clear()
    temp_store.clear()

    plan = rechunker.rechunk(
        ds,
        target_chunks=target_chunks,
        max_mem="6G",
        target_store=target_store,
        temp_store=temp_store,
    )

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
    spec["spec"]["scheduler"]["spec"]["containers"][0]["args"].extend(
        ["--idle-timeout", "600"]
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
            client.upload_file("rechunk_nwm.py")
            print("Dashboard Link:", client.dashboard_link)
            plan.execute(retries=10)


if __name__ == "__main__":
    sys.exit(main())
