"""
download public data sets from 17Lands.com and generate a card
file containing card attributes using MTGJSON
"""

import gzip
import os
import shutil
from enum import StrEnum

import wget
import polars as pl
from polars.exceptions import ComputeError

from spells import cards
from spells import cache
from spells.enums import View, ColName, EventType
from spells.schema import schema
from spells.draft_data import summon


DATASET_TEMPLATE = "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
RESOURCE_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
)

class FileFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


def _add(
    set_code: str,
    event_type: EventType,
    force_download: bool = False,
) -> int:
    mode = "refresh" if force_download else "add"
    cache.spells_print(mode, f"Adding {set_code} {event_type} to {cache.external_set_path(set_code)}")

    download_data_set(
        set_code, View.DRAFT, event_type=event_type, force_download=force_download
    )
    draft_names = cards.names_from_parquet(set_code, event_type)
    cards.write_card_file(set_code, draft_names, force_download=force_download)
    download_data_set(
        set_code, View.GAME, event_type=event_type, force_download=force_download
    )

    if event_type == EventType.PICK_TWO:
        cache.spells_print(
            "add",
            f"Skipping set context for {event_type} "
            "(summon does not support multi-pick formats yet)",
        )
    else:
        get_set_context(
            set_code, event_type=event_type, force_download=force_download
        )
    return 0


def _add_card_only(set_code: str, event_type: EventType) -> int:
    mode = "add"
    cache.spells_print(
        mode, f"Checking card file for {set_code} against existing {event_type} draft data"
    )
    try:
        names = cards.names_from_parquet(set_code, event_type)
    except FileNotFoundError as e:
        cache.spells_print("error", str(e))
        return 1

    # no force_download: builds the file if missing, validates and raises on
    # mismatch if it already exists — a spot check, not a rebuild
    return cards.write_card_file(set_code, names)


def _refresh(set_code: str, event_type: EventType):
    return _add(set_code, event_type=event_type, force_download=True)


def _refresh_card_only(set_code: str, event_type: EventType) -> int:
    mode = "refresh"
    cache.spells_print(
        mode, f"Rebuilding card file for {set_code} from existing {event_type} draft data"
    )
    try:
        names = cards.names_from_parquet(set_code, event_type)
    except FileNotFoundError as e:
        cache.spells_print("error", str(e))
        return 1

    cards.write_card_file(set_code, names, force_download=True)
    cache.clean(set_code)
    return 0


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
    event_type: EventType,
    force_download=False,
    clear_set_cache=True,
):
    mode = "refresh" if force_download else "add"
    cache.spells_print(
        mode,
        f"Downloading {set_code} {event_type} {dataset_type} dataset from 17Lands.com",
    )

    if not os.path.isdir(set_dir := cache.external_set_path(set_code)):
        os.makedirs(set_dir)

    target_path = cache.data_file_path(set_code, dataset_type, event_type)

    if os.path.isfile(target_path) and not force_download:
        cache.spells_print(
            mode,
            f"File {target_path} already exists, use `spells refresh {set_code}` to overwrite",
        )
        return 1

    dataset_file = DATASET_TEMPLATE.format(
        set_code=set_code, dataset_type=dataset_type, event_type=event_type
    )
    source_url = RESOURCE_TEMPLATE.format(dataset_type=dataset_type) + dataset_file
    dataset_path = os.path.join(cache.external_set_path(set_code), dataset_file)
    cache.spells_print(mode, f"Fetching {source_url}")
    wget.download(source_url, out=dataset_path)
    print()

    cache.spells_print(
        mode, "Unzipping and transforming to parquet (this might take a few minutes)..."
    )
    _process_zipped_file(dataset_path, target_path)
    cache.spells_print(mode, f"Wrote file {target_path}")
    if clear_set_cache:
        cache.clean(set_code)

    return 0


def get_set_context(
    set_code: str,
    event_type: EventType,
    force_download=False,
) -> int:
    mode = "refresh" if force_download else "add"

    context_fp = cache.data_file_path(set_code, "context", event_type)
    cache.spells_print(mode, "Calculating set context")
    if os.path.isfile(context_fp) and not force_download:
        cache.spells_print(
            mode,
            f"File {context_fp} already exists, use `spells refresh {set_code}` to overwrite",
        )
        return 1

    df = summon(
        set_code,
        columns=[ColName.NUM_TAKEN],
        group_by=[ColName.DRAFT_DATE, ColName.PICK_NUM],
        event_type=event_type,
    )

    context_df = df.filter(pl.col(ColName.NUM_TAKEN) > 1000).select(
        [
            pl.col(ColName.DRAFT_DATE).min().alias("release_date"),
            pl.col(ColName.PICK_NUM).max().alias("picks_per_pack"),
        ]
    )

    context_df.write_parquet(context_fp)

    cache.spells_print(mode, f"Wrote file {context_fp}")
    
    return 0
