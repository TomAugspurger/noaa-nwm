import os
import sys
import argparse
import rechunker
import zarr
import xarray as xr
import fsspec


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("product", choices=["channel_rt", "land"])

    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)
    product = args.product

    credential = os.environ["AZURE_SAS_TOKEN"]

    m = fsspec.get_mapper(
        "reference://",
        fo=f"abfs://ciroh/short-range-{product}-kerchunk/reference.json",
        remote_options={"account_name": "noaanwm"},
        target_options={"account_name": "noaanwm"},
        skip_instance_cache=True,
    )

    ds = xr.open_dataset(m, engine="zarr", consolidated=False, chunks={"time": 1})
    del ds.crs.encoding["_FillValue"]  # encoding issue

    match product:
        case "channel_rt":
            target_chunks = {"time": 6866, "feature_id": 100}
        case "land":
            target_chunks = {"time": 6866, "x": 100, "y": 100}
        case _:
            raise ValueError(f"Unknown product {product}")

    target_store = fsspec.get_mapper(f"abfs://ciroh/rechunk/{product}/target", account_name="noaanwm", credential=credential)
    temp_store = fsspec.get_mapper(f"abfs://ciroh/rechunk/{product}/temp", account_name="noaanwm", credential=credential)
    temp_store.clear()
 
    plan = rechunker.rechunk(ds, target_chunks=target_chunks, max_mem="6G", target_store=target_store, temp_store=temp_store)




if __name__ == "__main__":
    sys.exit(main())
