import numpy as np
import os
import zarr
import sys
import argparse
import xarray as xr
import fsspec
import dask.distributed
from dask_kubernetes.operator import KubeCluster
import dask.array as da


def parse_args(args=None):
    parser = argparse.ArgumentParser()

    return parser.parse_args(args)


def store_chunk(x, *, target, block_info):
    d = block_info[0]
    slices = tuple(slice(*loc) for loc in d["array-location"])
    target[slices] = x
    # Rely on the broadcast trick to not blow up memory?
    output = np.broadcast_to(np.array(1, x.dtype), x.shape)
    return output


def to_zarr(x, store, path, **kwargs):
    target = zarr.open_array(store, path=path, **kwargs)
    return da.map_blocks(store_chunk, x, target=target, meta=x._meta)


def main(args=None):
    credential = os.environ["AZURE_SAS_TOKEN"]
    storage_options = {"account_name": "noaanwm", "credential": credential}

    ds = xr.open_dataset(
        "az://ciroh/zarr/ts/short-range-forcing.zarr",
        engine="zarr",
        storage_options=storage_options,
        chunks={},
    )
    target_chunks = {"time": 168, "y": 240, "x": 288}
    store = fsspec.get_mapper(
        "az://ciroh/zarr/ts/short-range-forcing-rechunked.zarr", **storage_options
    )

    with dask.config.set(
        {"array.rechunk.method": "p2p", "optimization.fuse.active": False}
    ):
        ds2 = ds.chunk(target_chunks)

    for k, v in ds2.data_vars.items():
        v.encoding["chunks"] = tuple(target_chunks.values())
        v.encoding["preferred_chunks"] = target_chunks

    # write the metadata
    ds2.to_zarr(store, mode="w", consolidated=True, compute=False)

    # copy the data
    with KubeCluster(custom_cluster_spec="cluster.yaml") as cluster:
        with cluster.get_client() as client:
            print("Dashboard Link", cluster.dashboard_link)
            client.upload_file("rechunk_nwm.py")
            with dask.config.set(
                {"array.rechunk.method": "p2p", "optimization.fuse.active": False}
            ):
                for k, v in ds2.data_vars.items():
                    if v.dims == ("time", "y", "x"):
                        job = to_zarr(v.data, store=store, path=k)
                        target = zarr.open_array(store, path=k)
                        # We can't do that.
                        job = v.data.map_blocks(
                            store_chunk, target=target, meta=v.data._meta
                        )
                        print("execute", k)
                        job_ = job.persist()
                        dask.distributed.wait(job_)


if __name__ == "__main__":
    sys.exit(main())
