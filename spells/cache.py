"""
Module for caching the result of distributed dataframe calculations to parquet files.

Caches are keyed by a hash that is function of set code, aggregation type, base filter,
and groupbys.

Caches are cleared per-set when new files are downloaded.
"""

import os

import polars as pl

DATA = "data"
EXT = "local"


def data_dir_path(ext: str) -> str:
    """
    Where 17Lands data is stored. MDU_DATA_DIR environment variable is used, if it exists,
    otherwise the cwd is used
    """
    data_dir = os.path.join(DATA, ext)
    if project_dir := os.environ.get("MDU_PROJECT_DIR"):
        return os.path.join(project_dir, data_dir)
    return data_dir


def cache_dir_for_set(set_code: str) -> str:
    return os.path.join(data_dir_path(EXT), set_code)


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
    if not os.path.isdir(data_dir_path(EXT)):
        os.mkdir(data_dir_path(EXT))
    if not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    df.write_parquet(cache_path_for_key(set_code, cache_key))


def clear(set_code: str) -> int:
    if os.path.isdir(cache_dir_for_set(set_code)):
        with os.scandir(cache_dir_for_set(set_code)) as set_dir:
            count = 0
            for entry in set_dir:
                count += 1
                os.remove(entry)
            print(f"clear-cache: Removed {count} files from local cache for set {set_code}")
        return 0
    else:
        print(f"clear-cache: No local cache found for set {set_code}")
        return 0
