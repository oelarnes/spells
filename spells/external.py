"""
download public data sets from 17Lands.com and generate a card
file containing card attributes using MTGJSON

cli tool `spells`
"""

import csv
import functools
import gzip
import os
import re
import shutil
import sys
from enum import StrEnum

import wget
import polars as pl

from spells import cards
from spells import cache
from spells.enums import View
from spells.schema import schema


DATASET_TEMPLATE = "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
RESOURCE_TEMPLATE = "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"

class FileFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"

class EventType(StrEnum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"


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
            spells info

    add: Download draft and game files from 17Lands.com and card file from MTGJSON.com and save to path 
        [data home]/external/[set code] (use $SPELLS_DATA_HOME or $XDG_DATA_HOME to configure). 
        Does not overwrite existing files. If any files are downloaded, existing local cache is cleared.
        [set code] should be the capitalized official set code for the draft release.

        e.g. $ spells add OTJ

    refresh: Force download and overwrite of existing files (for new data drops, use sparingly!). Clear 
        local cache.

    remove: Delete the [data home]/external/[set code] and [data home]/local/[set code] directories and their contents

    clean: Delete [data home]/local/[set code] data directory (your cache of aggregate parquet files).

    info: No set code argument. Print info on all external and local files.
    """
    print_usage = functools.partial(cache.spells_print, "usage", usage)

    if len(sys.argv) < 2:
        print_usage()
        return 1

    mode = sys.argv[1]

    if mode == "info":
        return _info()

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
            return cache.clear(sys.argv[2])
        case _:
            print_usage()
            return 1


def _add(set_code: str, force_download=False):
    download_data_set(set_code, View.DRAFT, force_download=force_download)
    write_card_file(set_code, force_download=force_download)
    download_data_set(set_code, View.GAME, force_download=force_download)
    return 0


def _refresh(set_code: str):
    return _add(set_code, force_download=True)


def _remove(set_code: str):
    mode = "remove"
    dir_path = _external_set_path(set_code)
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

    return cache.clear(set_code)


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
                    if file_count < 3:
                        suggest_add.add(entry.name)
                    if file_count > 3:
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


def _external_set_path(set_code):
    return os.path.join(cache.data_dir_path(cache.DataDir.EXTERNAL), set_code)


def data_file_path(
    set_code, dataset_type: str, event_type=EventType.PREMIER
):
    if dataset_type == "card":
        return os.path.join(_external_set_path(set_code), f"{set_code}_card.parquet")

    return os.path.join(
        _external_set_path(set_code),
        f"{set_code}_{event_type}_{dataset_type}.parquet"
    )


def _process_zipped_file(gzip_path, target_path):
    csv_path = gzip_path[:-3]
    # if polars supports streaming from file obj, we can just stream straight
    # from urllib.Request through GzipFile to sink_parquet without intermediate files
    with gzip.open(gzip_path, "rb") as f_in:
        with open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)  # type: ignore
    
    os.remove(gzip_path)
    df = pl.scan_csv(csv_path, schema=schema(csv_path))
    df.sink_parquet(target_path)

    os.remove(csv_path)


def download_data_set(
    set_code,
    dataset_type: View,
    event_type=EventType.PREMIER,
    force_download=False,
    clear_set_cache=True,
):
    mode = "refresh" if force_download else "add"
    cache.spells_print(mode, f"Downloading {dataset_type} dataset from 17Lands.com")

    if not os.path.isdir(set_dir := _external_set_path(set_code)):
        os.makedirs(set_dir)

    target_path = data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        cache.spells_print(
            mode,
            f"File {target_path} already exists, use `spells refresh {set_code}` to overwrite",
        )
        return 1

    dataset_file = DATASET_TEMPLATE.format(set_code=set_code, dataset_type=dataset_type, event_type=event_type)
    dataset_path = os.path.join(_external_set_path(set_code), dataset_file)
    wget.download(
        RESOURCE_TEMPLATE.format(
            dataset_type=dataset_type 
        ) + dataset_file,
        out=dataset_path
    )
    print()

    cache.spells_print(mode, "Unzipping and transforming to parquet...")
    _process_zipped_file(dataset_path, target_path)
    cache.spells_print(mode, f"File {target_path} written")
    if clear_set_cache:
        cache.clear(set_code)

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
    card_filepath = data_file_path(draft_set_code, View.CARD)
    if os.path.isfile(card_filepath) and not force_download:
        cache.spells_print(
            mode,
            f"File {card_filepath} already exists, use `spells refresh {draft_set_code}` to overwrite",
        )
        return 1

    draft_filepath = data_file_path(draft_set_code, View.DRAFT)

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
