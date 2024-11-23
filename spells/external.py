"""
download public data sets from 17Lands.com and generate a card
file containing card attributes using MTGJSON
"""

import csv
import gzip
import os
import re
import shutil
import sys
import urllib.request
from enum import Enum

from spells import cards
from spells import cache
from spells.enums import View

import wget

DATA = "data"
EXTERNAL = "external"

URL_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
    + "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
)


class EventType(Enum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"


def cli() -> int:
    if os.getcwd().endswith('/spells') and os.path.isfile("__init__.py"):
        print("I think you found yourself inside of the spells module source directory. "
              "You should be up a level in the project root. Did you accidentally type `spells` and change directory?")
        return 1

    usage = """
usage: spells [add|refresh|clear-cache] [set_code]

    add: Download draft and game files from 17Lands.com and card file from MTGJSON.com and save to path 
        $SPELLS_PROJECT_DIR/external/[set code] (default $PWD/data). Does not overwrite existing files. 
        If any files are downloaded, existing local cache is cleared.

        e.g. >> spells add OTJ

    refresh: Force download and overwrite of existing files (for new data drops, use sparingly!). Clear 
        local cache.

    clear-cache: Delete local/[set code] data directory (your cache of aggregate parquet files).

    info: Print info on the external and local files for [set_code]

    --- 
    Note: To remove a set's data, just do `rm -r` on the data/external/[set code] path. I don't want to be responsible for deleting your data!

    (set_code should be the capitalized official set code for the draft release)
    """
    if len(sys.argv) != 3:
        print(usage)
        return 1

    mode = sys.argv[1]

    match mode:
        case "add":
            return _add(sys.argv[2])
        case "refresh":
            return _refresh(sys.argv[2])
        case "remove":
            return _remove(sys.argv[2])
        case "clear-cache":
            return cache.clear(sys.argv[2])
        case "info":
            return _info(sys.argv[2])
        case _:
            print(usage)
            return 1


def _add(set_code: str, force_download=False):
    download_data_set(set_code, View.DRAFT, force_download=force_download)
    download_data_set(set_code, View.GAME, force_download=force_download)
    write_card_file(set_code, force_download=force_download)
    return 0

def _refresh(set_code: str):
    return _add(set_code, force_download=True)

def _remove(set_code: str):
    print("hello from remove")
    return 0

def _info(set_code: str):
    print("hello from info")
    return 0

def _external_set_path(set_code):
    return os.path.join(cache.data_dir_path(EXTERNAL), set_code)

def data_file_path(set_code, dataset_type: str, event_type=EventType.PREMIER, zipped=False):
    if dataset_type == "card":
        return os.path.join(_external_set_path(set_code), f"{set_code}_card.csv")

    return os.path.join(
        _external_set_path(set_code),
        f"{set_code}_{event_type.value}_{dataset_type}.csv{'.gz' if zipped else ''}",
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
    print(f"{mode}: Downloading {dataset_type} dataset")

    if not os.path.isdir(target_dir := cache.data_dir_path(EXTERNAL)):
        print(f"{mode}: Creating data directory at {target_dir}. If you think you already have one, check your cwd and move your files")
        os.makedirs(target_dir)
    if not os.path.isdir(set_dir := _external_set_path(set_code)):
        os.makedirs(set_dir)

    target_path_zipped = data_file_path(set_code, dataset_type, zipped=True)
    target_path = data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        print(f"{mode}: File {target_path} already exists, use `spells refresh {set_code}` to overwrite")
        return 1

    wget.download(
        URL_TEMPLATE.format(
            set_code=set_code, dataset_type=dataset_type, event_type=event_type.value
        ),
        out=target_path_zipped,
    )
    print()

    _process_zipped_file(target_path_zipped, target_path)
    print(f"{mode}: File {target_path} written")
    if clear_set_cache:
        cache.clear(set_code)

    return 0

def write_card_file(draft_set_code: str, force_download=False) -> int:
    """
    Write a csv containing basic information about draftable cards, such as rarity,
    set symbol, color, mana cost, and type.
    """
    mode = "refresh" if force_download else "add"

    print(f"{mode}: Downloading card data and writing card csv file")
    card_filepath = data_file_path(draft_set_code, View.CARD)
    if os.path.isfile(card_filepath) and not force_download:
        print(f"{mode}: File {card_filepath} already exists, use `spells refresh {draft_set_code}` to overwrite")
        return 1

    draft_filepath = data_file_path(draft_set_code, View.DRAFT)

    if not os.path.isfile(draft_filepath):
        print(f"{mode}: Error: No draft file for set {draft_set_code}")
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

    print(f"{mode}: Wrote {len(csv_lines)} lines to file {card_filepath}")
    return 0

