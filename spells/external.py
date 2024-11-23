"""
download public data sets from 17Lands.com and generate a card
file containing card attributes using MTGJSON
"""

import csv
import functools
import gzip
import os
import re
import shutil
import sys
from enum import StrEnum

from spells import cards
from spells import cache
from spells.enums import View

import wget

URL_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
    + "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
)


class EventType(StrEnum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"


def cli() -> int:
    data_dir = cache.data_home()
    cache.spells_print('spells', f"[data home]={data_dir}")
    print()
    usage = """spells [add|refresh|remove|clear-cache|info] [set_code]

    add: Download draft and game files from 17Lands.com and card file from MTGJSON.com and save to path 
        [data home]/external/[set code] (use $SPELLS_DATA_HOME or $XDG_DATA_HOME to configure). 
        Does not overwrite existing files. If any files are downloaded, existing local cache is cleared.
        [set code] should be the capitalized official set code for the draft release.

        e.g. >> spells add OTJ

    refresh: Force download and overwrite of existing files (for new data drops, use sparingly!). Clear 
        local cache.

    clear-cache: Delete [data home]/local/[set code] data directory (your cache of aggregate parquet files).

    remove: Delete the [data home]/external/[set code] and [data home]/local/[set code] directories and their contents

    info: No set code argument. Print info on all external and local files.
    """
    print_usage = functools.partial(cache.spells_print, 'usage', usage)

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
        case "clear-cache":
            return cache.clear(sys.argv[2])
        case _:
            print_usage()
            return 1


def _add(set_code: str, force_download=False):
    download_data_set(set_code, View.DRAFT, force_download=force_download)
    download_data_set(set_code, View.GAME, force_download=force_download)
    write_card_file(set_code, force_download=force_download)
    return 0

def _refresh(set_code: str):
    return _add(set_code, force_download=True)

def _remove(set_code: str):
    mode = 'remove'
    dir_path = _external_set_path(set_code)
    if os.path.isdir(dir_path):
        with os.scandir(dir_path) as set_dir:
            count = 0
            for entry in set_dir:
                if not entry.name.endswith('.csv'):
                    cache.spells_print(mode, f"Unexpected file {entry.name} found in external cache, please sort that out!")
                    return 1
                count += 1
                os.remove(entry)
            cache.spells_print(mode, f"Removed {count} files from external cache for set {set_code}")
        os.rmdir(dir_path)
    else:
        cache.spells_print(mode, f"No external cache found for set {set_code}")

    return cache.clear(set_code, remove_dir=True)

def _info():
    print("hello from info")
    return 0

def _external_set_path(set_code):
    return os.path.join(cache.data_dir_path(cache.DataDir.EXTERNAL), set_code)

def data_file_path(set_code, dataset_type: str, event_type=EventType.PREMIER, zipped=False):
    if dataset_type == "card":
        return os.path.join(_external_set_path(set_code), f"{set_code}_card.csv")

    return os.path.join(
        _external_set_path(set_code),
        f"{set_code}_{event_type}_{dataset_type}.csv{'.gz' if zipped else ''}",
    )


def _process_zipped_file(target_path_zipped, target_path):
    with gzip.open(target_path_zipped, 'rb') as f_in:
       with open(target_path, 'wb') as f_out:
           shutil.copyfileobj(f_in, f_out) # type: ignore

    os.remove(target_path_zipped)


def download_data_set(
    set_code, dataset_type: View, event_type=EventType.PREMIER, force_download=False, clear_set_cache=True
):
    mode = "refresh" if force_download else "add"
    cache.spells_print(mode, f"Downloading {dataset_type} dataset")

    if not os.path.isdir(set_dir := _external_set_path(set_code)):
        os.makedirs(set_dir)

    target_path_zipped = data_file_path(set_code, dataset_type, zipped=True)
    target_path = data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        cache.spells_print(mode, f"File {target_path} already exists, use `spells refresh {set_code}` to overwrite")
        return 1

    wget.download(
        URL_TEMPLATE.format(
            set_code=set_code, dataset_type=dataset_type, event_type=event_type
        ),
        out=target_path_zipped,
    )
    print()

    _process_zipped_file(target_path_zipped, target_path)
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

    cache.spells_print(mode, "Downloading card data and writing card csv file")
    card_filepath = data_file_path(draft_set_code, View.CARD)
    if os.path.isfile(card_filepath) and not force_download:
        cache.spells_print(mode, f"File {card_filepath} already exists, use `spells refresh {draft_set_code}` to overwrite")
        return 1

    draft_filepath = data_file_path(draft_set_code, View.DRAFT)

    if not os.path.isfile(draft_filepath):
        cache.spells_print(mode, f"Error: No draft file for set {draft_set_code}")
        return 1

    with open(draft_filepath, encoding="utf-8") as f:
        columns = csv.DictReader(f).fieldnames

    if columns is None:
        raise ValueError("no columns found!")

    pattern = "^pack_card_"
    names = (re.split(pattern, name)[1] for name in columns if re.search(pattern, name) is not None)

    csv_lines = cards.card_file_lines(draft_set_code, names)

    with open(card_filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in csv_lines:
            writer.writerow(row)

    cache.spells_print(mode, f"Wrote {len(csv_lines)} lines to file {card_filepath}")
    return 0

