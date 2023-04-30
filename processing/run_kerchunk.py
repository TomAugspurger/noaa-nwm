import argparse
import os
import pathlib
import sys

from dask_kubernetes.operator import KubeCluster
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, MetadataTarget
import fsspec
import tlz
from pangeo_forge_recipes.recipes.reference_hdf_zarr import (
    ChunkKey,
    Stage,
    unstrip_protocol,
    file_opener,
    create_kerchunk_reference,
)
from pangeo_forge_recipes.recipes.reference_hdf_zarr import Pipeline, finalize


# workaround for https://github.com/pangeo-forge/pangeo-forge-recipes/issues/515
def scan_file(chunk_key: ChunkKey, config: HDFReferenceRecipe):
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    fname = config.file_pattern[chunk_key]
    # ref_fname = os.path.basename(fname + ".json")
    ref_fname = fname + ".json"
    with file_opener(fname, **config.netcdf_storage_options) as fp:
        protocol = getattr(getattr(fp, "fs", None), "protocol", None)  # make mypy happy
        if protocol is None:
            raise ValueError("Couldn't determine protocol")
        target_url = unstrip_protocol(fname, protocol)
        config.storage_config.metadata[ref_fname] = create_kerchunk_reference(
            fp,
            target_url,
            file_type=config.file_pattern.file_type,
            inline_threshold=config.inline_threshold,
        )


# workaround for https://github.com/pangeo-forge/pangeo-forge-recipes/issues/515
def hdf_reference_recipe_compiler(recipe: HDFReferenceRecipe) -> Pipeline:
    stages = [
        Stage(
            name="scan_file", function=scan_file, mappable=list(recipe.iter_inputs())
        ),
        Stage(name="finalize", function=finalize),
    ]
    return Pipeline(stages=stages, config=recipe)


# workaround for https://github.com/pangeo-forge/pangeo-forge-recipes/issues/515
class MyHDFReferenceRecipe(HDFReferenceRecipe):
    _compiler = hdf_reference_recipe_compiler


# Workaround https://github.com/pangeo-forge/pangeo-forge-recipes/issues/419
class MyTarget(FSSpecTarget):
    def __post_init__(self):
        pass


# Workaround https://github.com/pangeo-forge/pangeo-forge-recipes/issues/419
class MyMetadataTarget(MetadataTarget):
    def __post_init__(self):
        pass


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("product", choices=["channel_rt", "land", "forcing"])

    return parser.parse_args(args)


def is_bad(f):
    """
    Check if a NetCDF file is corrupt.
    """
    import xarray as xr

    try:
        xr.open_dataset(fsspec.open(f, account_name="noaanwm").open())
    except Exception:
        return f
    else:
        return False


def list_day(root, product):
    """
    List the files for a product under a day prefix.
    """
    fs = fsspec.filesystem("abfs", account_name="noaanwm")
    if product == "forcing":
        # nwm/nwm.20230123/forcing_short_range/nwm.t00z.short_range.forcing.f001.conus.nc  # noqa: E501
        pattern = f"{root}/forcing_short_range/nwm.t*z.short_range.forcing.f001.conus.nc"  # noqa: E501
    else:
        # nwm/nwm.20230123/short_range/nwm.t00z.short_range.channel_rt.f001.conus.nc
        pattern = f"{root}/short_range/nwm.*.short_range.{product}.f001.conus.nc"

    return fs.glob(pattern)


def main(args=None):
    args = parse_args(args)
    product = args.product

    credential = os.environ["AZURE_SAS_TOKEN"]

    p = pathlib.Path(f"{product}-files.txt")

    if not p.exists():
        # there are missing days / hours, so we can't use a simple file pattern.
        print("Listing files")
        with KubeCluster(
            image="pccomponentstest.azurecr.io/noaa-nwm:2023.4.26.0",
            resources={
                "requests": {"memory": "7Gi", "cpu": "0.9"},
                "limit": {"memory": "8Gi", "cpu": "1"},
            },
            worker_command="dask-worker --nthreads 1 --nworkers 1 --memory-limit 8GB",
        ) as cluster:
            cluster.scale(8)
            with cluster.get_client() as client:
                client.upload_file("run_kerchunk.py")
                print("Dashboard Link:", client.dashboard_link)

                fs = fsspec.filesystem("abfs", account_name="noaanwm")
                roots = fs.glob("nwm/nwm.*")  # fast, single-threaded
                futures = client.map(list_day, roots, product=product)  # parallel
                files_lists = client.gather(futures)
                file_list = tlz.concat(files_lists)

        p.write_text("\n".join(file_list))

    file_list = p.read_text().split("\n")

    bad = {
        "nwm/nwm.20220917/short_range/nwm.t18z.short_range.channel_rt.f001.conus.nc",
        "nwm/nwm.20220926/short_range/nwm.t16z.short_range.channel_rt.f001.conus.nc",
        "nwm/nwm.20220913/short_range/nwm.t12z.short_range.land.f001.conus.nc",
        "nwm/nwm.20220927/short_range/nwm.t20z.short_range.land.f001.conus.nc",
        "nwm/nwm.20221020/forcing_short_range/nwm.t00z.short_range.forcing.f001.conus.nc",
    }

    file_list = [
        x
        for x in file_list
        # https://github.com/pangeo-forge/staged-recipes/pull/215/#issuecomment-1520905668
        # filter to newer files
        if x.split("/")[1].split(".")[1] > "20220628"
        # drop corrupt NetCDF files
        and x not in bad
    ]
    print(f"Processing {len(file_list)} files")

    urls = ["abfs://" + f for f in file_list]

    # Create filepattern from urls
    pattern = pattern_from_file_sequence(urls, "time")

    match product:
        case "channel_rt":
            identical_dims = ["feature_id"]
        case "land":
            identical_dims = ["x", "y"]
        case "forcing":
            identical_dims = ["x", "y", "crs"]
        case _:
            raise ValueError(f"Unknown product {product}")

    # Create HDFReference recipe from pattern
    recipe = MyHDFReferenceRecipe(
        pattern,
        netcdf_storage_options={"account_name": "noaanwm"},
        identical_dims=identical_dims,
    )

    # configure storage
    target_storage_options = dict(account_name="noaanwm", credential=credential)
    target_fs = fsspec.filesystem("abfs", **target_storage_options)
    storage = StorageConfig(
        target=MyTarget(target_fs, root_path=f"ciroh/short-range-{product}-kerchunk/"),
        metadata=MyMetadataTarget(
            target_fs, root_path=f"ciroh/short-range-{product}-kerchunk-metadata/"
        ),
    )
    recipe.storage_config = storage

    # Run it
    with KubeCluster(
        image="pccomponentstest.azurecr.io/noaa-nwm:2023.4.26.0",
        resources={
            "requests": {"memory": "7Gi", "cpu": "0.9"},
            "limit": {"memory": "8Gi", "cpu": "1"},
        },
        worker_command="dask-worker --nthreads 1 --nworkers 1 --memory-limit 8GB",
    ) as cluster:
        cluster.scale(64)
        with cluster.get_client() as client:
            client.upload_file("run_kerchunk.py")
            print("Dashboard Link:", client.dashboard_link)
            recipe.to_dask().compute()


if __name__ == "__main__":
    sys.exit(main())
