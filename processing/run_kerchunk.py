import argparse
import os
import pathlib
import sys

from dask_kubernetes.operator import KubeCluster
from distributed import Client
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, MetadataTarget
import fsspec
import tlz
from pangeo_forge_recipes.recipes.reference_hdf_zarr import (
    ChunkKey,
    HDFReferenceRecipe,
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


def hdf_reference_recipe_compiler(recipe: HDFReferenceRecipe) -> Pipeline:
    stages = [
        Stage(
            name="scan_file", function=scan_file, mappable=list(recipe.iter_inputs())
        ),
        Stage(name="finalize", function=finalize),
    ]
    return Pipeline(stages=stages, config=recipe)


class MyHDFReferenceRecipe(HDFReferenceRecipe):
    _compiler = hdf_reference_recipe_compiler


# Workaround https://github.com/pangeo-forge/pangeo-forge-recipes/issues/419
class MyTarget(FSSpecTarget):
    def __post_init__(self):
        pass


class MyMetadataTarget(MetadataTarget):
    def __post_init__(self):
        pass


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("product", choices=["channel_rt", "land"])

    return parser.parse_args(args)


def list_day(root, product):
    fs = fsspec.filesystem("abfs", account_name="noaanwm")
    return fs.glob(f"{root}/short_range/nwm.*.short_range.{product}.f001.conus.nc")


def main(args=None):
    args = parse_args(args)
    product = args.product

    credential = os.environ["AZURE_SAS_TOKEN"]

    p = pathlib.Path(f"{product}-files.txt")

    if not p.exists():
        # there are missing days / hours, so we can't use a simple file pattern.
        print("Listing files")
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

                fs = fsspec.filesystem("abfs", account_name="noaanwm")
                roots = fs.glob("nwm/nwm.*")  # fast, single-threaded
                futures = client.map(list_day, roots, product=product)  # parallel
                files_lists = client.gather(futures)
                file_list = tlz.concat(files_lists)

        p.write_text("\n".join(file_list))

    file_list = p.read_text().split("\n")

    # workaround https://github.com/pangeo-forge/staged-recipes/pull/215/#issuecomment-1520905668
    # by filtering to newer files
    # also drop corrupt NetCDF files
    bad = {
        "nwm/nwm.20220917/short_range/nwm.t18z.short_range.channel_rt.f001.conus.nc",
        "nwm/nwm.20220926/short_range/nwm.t16z.short_range.channel_rt.f001.conus.nc",
    }

    file_list = [
        x
        for x in file_list
        if x.split("/")[1].split(".")[1] > "20220628"
        and x[61:65] == "f001"
        and x not in bad
    ]
    print(f"Processing {len(file_list)} files")

    fs = fsspec.filesystem("abfs", account_name="noaanwm")
    urls = ["abfs://" + f for f in file_list]

    # Create filepattern from urls
    pattern = pattern_from_file_sequence(urls, "time")

    if product == "channel_rt":
        identical_dims = ["feature_id"]
    else:
        identical_dims = ["x", "y"]

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
            recipe.to_dask().compute()


if __name__ == "__main__":
    sys.exit(main())
