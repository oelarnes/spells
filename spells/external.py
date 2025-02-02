"""
download public data sets from 17Lands.com and generate a card
file containing card attributes using MTGJSON

cli tool `spells`
"""

import functools
import gzip
import os
import re
import shutil
import sys
from enum import StrEnum

import wget
import polars as pl
from polars.exceptions import ComputeError

from spells import cards
from spells import cache
from spells.config import all_sets
from spells.enums import View, ColName
from spells.schema import schema
from spells.draft_data import summon


DATASET_TEMPLATE = "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
RESOURCE_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
)


class FileFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


# Fred Cirera via https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def cli() -> int:
    data_dir = cache.data_home()
    cache.spells_print("spells", f"[data home]={data_dir}")
    print()
    usage = """spells [add|refresh|remove|clean] [set_code]
            spells add all 
            spells clean all
            spells info

    add: Download draft and game files from 17Lands.com and card file from MTGJSON.com and save to path 
        [data home]/external/[set code] (use $SPELLS_DATA_HOME or $XDG_DATA_HOME to configure). 
        Does not overwrite existing files. If any files are downloaded, existing local cache is cleared.
        [set code] should be the capitalized official set code for the draft release.

        e.g. $ spells add OTJ

    refresh: Force download and overwrite of existing files (for new data drops, use sparingly!). Clear 
        local 

    remove: Delete the [data home]/external/[set code] and [data home]/local/[set code] directories and their contents

    clean: Delete [data home]/local/[set code] data directory (your cache of aggregate parquet files), or all of them.

    info: No set code argument. Print info on all external and local files.

    context: Refresh context files
    """
    print_usage = functools.partial(cache.spells_print, "usage", usage)

    if len(sys.argv) < 2:
        print_usage()
        return 1

    mode = sys.argv[1]

    if mode == "info":
        return _info()

    if mode == "context":
        return _context()

    if len(sys.argv) != 3:
        print_usage()
        return 1

    match mode:
        case "add":
            return _add(sys.argv[2])
        case "refresh":
            return _refresh(sys.argv[2])
        case "remove":
            return _remove(sys.argv[2])
        case "clean":
            return cache.clean(sys.argv[2])
        case _:
            print_usage()
            return 1


def _add(set_code: str, force_download: bool = False) -> int:
    if set_code == "all":
        for code in all_sets:
            _add(code, force_download=force_download)

    download_data_set(set_code, View.DRAFT, force_download=force_download)
    write_card_file(set_code, force_download=force_download)
    get_set_context(set_code, force_download=force_download)
    download_data_set(set_code, View.GAME, force_download=force_download)
    return 0


def _context() -> int:
    cache.spells_print("context", "Refreshing all context files")
    for code in all_sets:
        write_card_file(code, force_download=True)
        get_set_context(code, force_download=True)
    return 0 

def _refresh(set_code: str):
    return _add(set_code, force_download=True)


def _remove(set_code: str):
    mode = "remove"
    dir_path = cache.external_set_path(set_code)
    if os.path.isdir(dir_path):
        with os.scandir(dir_path) as set_dir:
            count = 0
            for entry in set_dir:
                if not entry.name.endswith(".parquet"):
                    cache.spells_print(
                        mode,
                        f"Unexpected file {entry.name} found in external cache, please sort that out!",
                    )
                    return 1
                count += 1
                os.remove(entry)
            cache.spells_print(
                mode, f"Removed {count} files from external cache for set {set_code}"
            )
        os.rmdir(dir_path)
    else:
        cache.spells_print(mode, f"No external cache found for set {set_code}")

    return cache.clean(set_code)


def _info():
    mode = "info"
    external_path = cache.data_dir_path(cache.DataDir.EXTERNAL)

    suggest_add = set()
    suggest_remove = set()
    all_external = set()
    if os.path.isdir(external_path):
        cache.spells_print(mode, f"External archives found {external_path}")
        with os.scandir(external_path) as ext_dir:
            for entry in ext_dir:
                if entry.is_dir():
                    all_external.add(entry.name)
                    file_count = 0
                    cache.spells_print(mode, f"Archive {entry.name} contents:")
                    for item in os.scandir(entry):
                        if not re.match(f"^{entry.name}_.*\\.parquet", item.name):
                            print(
                                f"!!! imposter file {item.name}! Please sort that out"
                            )
                        print(f"    {item.name} {sizeof_fmt(os.stat(item).st_size)}")
                        file_count += 1
                    if file_count < 4:
                        suggest_add.add(entry.name)
                    if file_count > 4:
                        suggest_remove.add(entry.name)
                else:
                    cache.spells_print(
                        mode, f"Imposter file {entry.name}! Please sort that out"
                    )

    else:
        cache.spells_print(mode, "No external archives found")

    cache_path = cache.data_dir_path(cache.DataDir.CACHE)

    if os.path.isdir(cache_path):
        print()
        cache.spells_print(mode, f"Local cache found {cache_path}")
        with os.scandir(cache_path) as cache_dir:
            for entry in cache_dir:
                if entry.name not in all_external:
                    suggest_remove.add(entry.name)
                if entry.is_dir():
                    cache.spells_print(mode, f"Cache {entry.name} contents:")
                    parquet_num = 0
                    parquet_size = 0
                    for item in os.scandir(entry):
                        if item.name.endswith(".parquet"):
                            parquet_num += 1
                            parquet_size += os.stat(item).st_size
                        else:
                            print(
                                f"!!! imposter file {item.name}! Please sort that out"
                            )
                    print(f"    {parquet_num} cache files: {sizeof_fmt(parquet_size)}")
    else:
        print()
        cache.spells_print(mode, "No local cache found")

    print()
    for name in suggest_add:
        cache.spells_print(mode, f"Suggest `spells add {name}'")
    for name in suggest_remove:
        cache.spells_print(mode, f"Suggest `spells remove {name}'")

    return 0


def _process_zipped_file(gzip_path, target_path):
    csv_path = gzip_path[:-3]
    # if polars supports streaming from file obj, we can just stream straight
    # from urllib.Request through GzipFile to sink_parquet without intermediate files
    with gzip.open(gzip_path, "rb") as f_in:
        with open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)  # type: ignore

    os.remove(gzip_path)
    df = pl.scan_csv(csv_path, schema=schema(csv_path))
    try:
        df.sink_parquet(target_path)
    except ComputeError:
        df = pl.scan_csv(csv_path)
        cache.spells_print(
            "error",
            "Bad schema found, loading dataset into memory"
            + " and attempting to cast to correct schema",
        )
        select = [pl.col(name).cast(dtype) for name, dtype in schema(csv_path).items()]
        cast_df = df.select(select).collect()
        cast_df.write_parquet(target_path)

    os.remove(csv_path)


def download_data_set(
    set_code,
    dataset_type: View,
    event_type=cache.EventType.PREMIER,
    force_download=False,
    clear_set_cache=True,
):
    mode = "refresh" if force_download else "add"
    cache.spells_print(mode, f"Downloading {dataset_type} dataset from 17Lands.com")

    if not os.path.isdir(set_dir := cache.external_set_path(set_code)):
        os.makedirs(set_dir)

    target_path = cache.data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        cache.spells_print(
            mode,
            f"File {target_path} already exists, use `spells refresh {set_code}` to overwrite",
        )
        return 1

    dataset_file = DATASET_TEMPLATE.format(
        set_code=set_code, dataset_type=dataset_type, event_type=event_type
    )
    dataset_path = os.path.join(cache.external_set_path(set_code), dataset_file)
    wget.download(
        RESOURCE_TEMPLATE.format(dataset_type=dataset_type) + dataset_file,
        out=dataset_path,
    )
    print()

    cache.spells_print(
        mode, "Unzipping and transforming to parquet (this might take a few minutes)..."
    )
    _process_zipped_file(dataset_path, target_path)
    cache.spells_print(mode, f"Wrote file {target_path}")
    if clear_set_cache:
        cache.clean(set_code)

    return 0


def write_card_file(draft_set_code: str, force_download=False) -> int:
    """
    Write a csv containing basic information about draftable cards, such as rarity,
    set symbol, color, mana cost, and type.
    """
    mode = "refresh" if force_download else "add"

    cache.spells_print(
        mode, "Fetching card data from mtgjson.com and writing card file"
    )
    card_filepath = cache.data_file_path(draft_set_code, View.CARD)
    if os.path.isfile(card_filepath) and not force_download:
        cache.spells_print(
            mode,
            f"File {card_filepath} already exists, use `spells refresh {draft_set_code}` to overwrite",
        )
        return 1

    draft_filepath = cache.data_file_path(draft_set_code, View.DRAFT)

    if not os.path.isfile(draft_filepath):
        cache.spells_print(mode, f"Error: No draft file for set {draft_set_code}")
        return 1

    columns = pl.scan_parquet(draft_filepath).collect_schema().names()

    pattern = "^pack_card_"
    names = [
        re.split(pattern, name)[1]
        for name in columns
        if re.search(pattern, name) is not None
    ]

    card_df = cards.card_df(draft_set_code, names)

    card_df.write_parquet(card_filepath)

    cache.spells_print(mode, f"Wrote file {card_filepath}")
    return 0


def get_set_context(set_code: str, force_download=False) -> int:
    mode = "refresh" if force_download else "add"

    context_fp = cache.data_file_path(set_code, "context")
    cache.spells_print(mode, "Calculating set context")
    if os.path.isfile(context_fp) and not force_download:
        cache.spells_print(
            mode,
            f"File {context_fp} already exists, use `spells refresh {set_code}` to overwrite",
        )
        return 1

    df = summon(
        set_code,
        columns=[ColName.NUM_DRAFTS],
        group_by=[ColName.DRAFT_DATE, ColName.PICK_NUM],
    )

    context_df = df.filter(pl.col(ColName.NUM_DRAFTS) > 1000).select(
        [
            pl.col(ColName.DRAFT_DATE).min().alias("release_date"),
            pl.col(ColName.PICK_NUM).max().alias("picks_per_pack"),
        ]
    )

    context_df.write_parquet(context_fp)

    cache.spells_print(mode, f"Wrote file {context_fp}")
    return 0
