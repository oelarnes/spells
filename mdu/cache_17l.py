"""
download public data sets from 17Lands.com and generate a card
file containing card attributes
"""

import urllib.request
import os
import gzip
import re
import csv

from enum import Enum

from mdu.scryfall import Scryfall
from mdu.config.mdu_cfg import DRAFT_SET_SYMBOL_MAP
from mdu.cache import data_dir_path, clear_cache
from mdu.enums import View

DATA = "data"
C17_EXT = "17l-files"

URL_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
    + "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
)


class EventType(Enum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"


def data_file_path(set_code, dataset_type: str, event_type=EventType.PREMIER, zipped=False):
    if dataset_type == "card":
        return os.path.join(data_dir_path(C17_EXT), f"{set_code}_card.csv")

    return os.path.join(
        data_dir_path(C17_EXT),
        f"{set_code}_{event_type.value}_{dataset_type}.csv{'.gz' if zipped else ''}",
    )


def process_zipped_file(target_path_zipped, target_path):
    with gzip.open(target_path_zipped, "rt", newline="", encoding="utf-8") as f_in:
        with open(target_path, "w", newline="", encoding="utf-8") as f_out:
            # we are going to add an increasing draft_id index to the beginning
            # of the line to facilitate distributed grouping by draft_id
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            headers = next(reader)
            draft_id_loc = headers.index("draft_id")
            headers.insert(0, "draft_id_idx")
            writer.writerow(headers)

            draft_id_idx = 0
            draft_id = None
            for row in reader:
                if row[draft_id_loc] != draft_id:
                    draft_id = row[draft_id_loc]
                    draft_id_idx += 1
                row.insert(0, str(draft_id_idx))
                writer.writerow(row)

    os.remove(target_path_zipped)


def download_data_set(
    set_code, dataset_type: View, event_type=EventType.PREMIER, force_download=False, clear_set_cache=True
):
    if clear_set_cache:
        clear_cache(set_code)
    target_dir = data_dir_path(C17_EXT)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    target_path_zipped = data_file_path(set_code, dataset_type, zipped=True)
    target_path = data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        print("file %(target_path) already exists, rerun with force_download=True to download")
        return

    urllib.request.urlretrieve(
        URL_TEMPLATE.format(
            set_code=set_code, dataset_type=dataset_type, event_type=event_type.value
        ),
        target_path_zipped,
    )

    process_zipped_file(target_path_zipped, target_path)


def write_card_file(draft_set_code):
    """
    Write a csv containing basic information about draftable cards, such as rarity,
    set symbol, color, mana cost, and type.

    Gets names from the 17lands headers and card information from a cache of Scryfall
    in local mongo. (Requires a mongo instance populated with Scryfall data, see
    mdu.scryfall module)
    """
    draft_filepath = data_file_path(draft_set_code, View.DRAFT)

    if not os.path.isfile(draft_filepath):
        print(f"No draft file for set {draft_set_code}")

    with open(draft_filepath, encoding="utf-8") as f:
        columns = csv.DictReader(f).fieldnames

    if columns is None:
        raise ValueError("no columns found!")

    pattern = "^pack_card_"

    names = (re.split(pattern, name)[1] for name in columns if re.search(pattern, name) is not None)

    card_attrs = ["name", "set", "rarity", "color", "color_identity_str", "type", "subtype", "cmc"]

    card_file_rows = [",".join(card_attrs) + "\n"]
    sf = Scryfall(DRAFT_SET_SYMBOL_MAP[draft_set_code])
    for name in names:
        card = sf.get_card(name)
        if card is None:
            raise ValueError(f"Card name {name} not found, please update Scryfall cache")
        card_file_rows.append(card.attr_line(card_attrs) + "\n")

    with open(data_file_path(draft_set_code, "card"), "w", encoding="utf-8") as f:
        f.writelines(card_file_rows)
