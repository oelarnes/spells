"""
Module for caching the result of distributed dataframe calculations to parquet files.

Caches are keyed by a hash that is function of set code, aggregation type, base filter,
and groupbys.

Caches are cleared per-set when new files are downloaded.
"""

from enum import StrEnum
import os
import sys

import polars as pl


class EventType(StrEnum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"


class DataDir(StrEnum):
    CACHE = "cache"
    EXTERNAL = "external"


def spells_print(mode, content):
    print(f"  🪄 {mode} ✨ {content}")


def data_home() -> str:
    is_win = sys.platform == "win32"
    return os.path.expanduser(
        os.environ.get(
            "SPELLS_DATA_HOME",
            os.environ.get(
                "XDG_DATA_HOME",
                r"~\AppData\Local\Spells" if is_win else "~/.local/share/spells/",
            ),
        )
    )


def data_dir_path(cache_dir: DataDir) -> str:
    """
    Where 17Lands data is stored. MDU_DATA_DIR environment variable is used, if it exists,
    otherwise the cwd is used
    """
    is_win = sys.platform == "win32"

    ext = {
        DataDir.CACHE: "Cache" if is_win else "cache",
        DataDir.EXTERNAL: "External" if is_win else "external",
    }[cache_dir]

    data_dir = os.path.join(data_home(), ext)
    return data_dir


def external_set_path(set_code):
    return os.path.join(data_dir_path(DataDir.EXTERNAL), set_code)


def data_file_path(set_code, dataset_type: str, event_type=EventType.PREMIER):
    if dataset_type == "set_context":
        return os.path.join(external_set_path(set_code), f"{set_code}_context.parquet")

    if dataset_type == "card":
        return os.path.join(external_set_path(set_code), f"{set_code}_card.parquet")

    return os.path.join(
        external_set_path(set_code), f"{set_code}_{event_type}_{dataset_type}.parquet"
    )


def cache_dir_for_set(set_code: str) -> str:
    return os.path.join(data_dir_path(DataDir.CACHE), set_code)


def cache_path_for_key(set_code: str, cache_key: str) -> str:
    return os.path.join(cache_dir_for_set(set_code), cache_key + ".parquet")


def cache_exists(set_code: str, cache_key: str) -> bool:
    return os.path.isdir(cache_dir_for_set(set_code)) and os.path.isfile(
        cache_path_for_key(set_code, cache_key)
    )


def read_cache(set_code: str, cache_key: str) -> pl.DataFrame:
    return pl.read_parquet(cache_path_for_key(set_code, cache_key))


def write_cache(set_code: str, cache_key: str, df: pl.DataFrame) -> None:
    cache_dir = cache_dir_for_set(set_code)
    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)

    df.write_parquet(cache_path_for_key(set_code, cache_key))


def clean(set_code: str) -> int:
    mode = "clean"

    if set_code == "all":
        cache_dir = data_dir_path(DataDir.CACHE)
        with os.scandir(cache_dir) as set_dir:
            for entry in set_dir:
                clean(entry.name)
        return 0

    cache_dir = cache_dir_for_set(set_code)
    if os.path.isdir(cache_dir):
        with os.scandir(cache_dir) as set_dir:
            count = 0
            for entry in set_dir:
                if not entry.name.endswith(".parquet"):
                    spells_print(
                        mode,
                        f"Unexpected file {entry.name} found in local cache, please sort that out!",
                    )
                    return 1
                count += 1
                os.remove(entry)
            spells_print(
                mode, f"Removed {count} files from local cache for set {set_code}"
            )
        os.rmdir(cache_dir)
        spells_print(mode, f"Removed local cache dir {cache_dir}")
        return 0
    else:
        spells_print(mode, f"No local cache found for set {set_code}")
        return 0
