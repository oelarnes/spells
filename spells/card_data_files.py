import datetime as dt
import json
import logging
import os
from pathlib import Path
import wget
from time import sleep

import polars as pl

from spells import cache
from spells.enums import ColName, EventType

RATINGS_TEMPLATE = (
    "https://www.17lands.com/card_ratings/data?expansion={set_code}&format={format}"
    "{user_group_param}{deck_color_param}&start_date={start_date_str}&end_date={end_date_str}"
)

DECK_COLOR_DATA_TEMPLATE = (
    "https://www.17lands.com/color_ratings/data?expansion={set_code}&event_type={format}"
    "{user_group_param}&start_date={start_date_str}&end_date={end_date_str}&combine_splash=true"
)

FILTERS_URL = "https://www.17lands.com/data/filters"


ratings_col_defs = {
    ColName.NAME: pl.col("name").cast(pl.String),
    ColName.COLOR: pl.col("color").cast(pl.String),
    ColName.RARITY: pl.col("rarity").cast(pl.String),
    ColName.IMAGE_URL: pl.col("url").cast(pl.String),
    ColName.NUM_SEEN: pl.col("seen_count").cast(pl.Int64),
    ColName.LAST_SEEN: pl.col("seen_count") * pl.col("avg_seen").cast(pl.Float64),
    ColName.NUM_TAKEN: pl.col("pick_count").cast(pl.Int64),
    ColName.TAKEN_AT: pl.col("pick_count") * pl.col("avg_pick").cast(pl.Float64),
    ColName.DECK: pl.col("game_count").cast(pl.Int64),
    ColName.WON_DECK: pl.col("win_rate") * pl.col("game_count").cast(pl.Float64),
    ColName.SIDEBOARD: (pl.col("pool_count") - pl.col("game_count")).cast(pl.Int64),
    ColName.OPENING_HAND: pl.col("opening_hand_game_count").cast(pl.Int64),
    ColName.WON_OPENING_HAND: pl.col("opening_hand_game_count")
    * pl.col("opening_hand_win_rate").cast(pl.Float64),
    ColName.DRAWN: pl.col("drawn_game_count").cast(pl.Int64),
    ColName.WON_DRAWN: pl.col("drawn_win_rate")
    * pl.col("drawn_game_count").cast(pl.Float64),
    ColName.NUM_GIH: pl.col("ever_drawn_game_count").cast(pl.Int64),
    ColName.NUM_GIH_WON: pl.col("ever_drawn_game_count")
    * pl.col("ever_drawn_win_rate").cast(pl.Float64),
    ColName.NUM_GNS: pl.col("never_drawn_game_count").cast(pl.Int64),
    ColName.WON_NUM_GNS: pl.col("never_drawn_game_count")
    * pl.col("never_drawn_win_rate").cast(pl.Float64),
}

deck_color_col_defs = {
    ColName.MAIN_COLORS: pl.col("short_name").cast(pl.String),
    ColName.NUM_GAMES: pl.col("games").cast(pl.Int64),
    ColName.NUM_WON: pl.col("wins").cast(pl.Int64),
}


def download_data_file(url: str, target_dir: str, filename: str) -> str:
    """Download a 17lands data file unless already cached; return the local path."""
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    file_path = os.path.join(target_dir, filename)

    if not os.path.isfile(file_path):
        wget.download(url, out=file_path)

    return file_path


def _fetch_filters() -> dict:
    """Fetch 17lands' /data/filters endpoint (expansion list, format list, and each
    expansion's `start_date`), cached to one file per calendar day so it's fetched
    at most once a day. Falls back to the most recently cached file if the network
    is unavailable — since the endpoint always returns the full set of expansions,
    a stale cache still answers correctly for any expansion whose start_date isn't
    still changing (i.e. everything except a set that released within the gap since
    the last successful fetch).
    """
    target_dir, filename = cache.filters_file_path(dt.date.today())
    file_path = os.path.join(target_dir, filename)

    if os.path.isfile(file_path):
        return json.loads(Path(file_path).read_text())

    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    try:
        wget.download(FILTERS_URL, out=file_path)
        return json.loads(Path(file_path).read_text())
    except Exception as e:
        cached = sorted(Path(target_dir).glob("filters_*.json"))
        if not cached:
            raise RuntimeError(
                f"Could not fetch {FILTERS_URL} and no cached filters file exists"
            ) from e
        logging.warning(
            f"Fetching {FILTERS_URL} failed ({e}); using stale cache {cached[-1].name}"
        )
        return json.loads(cached[-1].read_text())


def expansion_start_date(set_code: str) -> dt.date:
    """The date 17lands considers a format to have started for `set_code`, per the
    same endpoint that defaults the date range on 17lands.com/card_data. Used to
    resolve a `CardDataFileSpec`'s `format_day`-relative window without requiring
    the caller to know or maintain each set's release date.
    """
    filters = _fetch_filters()
    start_dates = filters.get("start_dates", {})
    if set_code not in start_dates:
        raise ValueError(
            f"No known start date for set '{set_code}' from {FILTERS_URL}"
        )
    return dt.datetime.fromisoformat(start_dates[set_code]).date()


def deck_color_df(
    set_code: str,
    event_type: EventType = EventType.PREMIER,
    player_cohort: str = "all",
    *,
    start_date: dt.date,
    end_date: dt.date | None = None,
):
    if end_date is None:
        end_date = dt.date.today() - dt.timedelta(days=1)

    target_dir, filename = cache.deck_color_file_path(
        set_code,
        event_type,
        player_cohort,
        start_date,
        end_date,
    )

    user_group_param = (
        "" if player_cohort == "all" else f"&user_group={player_cohort}"
    )

    url = DECK_COLOR_DATA_TEMPLATE.format(
        set_code=set_code,
        format=event_type,
        user_group_param=user_group_param,
        start_date_str=start_date.strftime("%Y-%m-%d"),
        end_date_str=end_date.strftime("%Y-%m-%d"),
    )

    deck_color_file_path = download_data_file(url, target_dir, filename)

    df = (
        pl.read_json(deck_color_file_path)
        .filter(~pl.col("is_summary"))
        .select(
            [
                pl.lit(set_code).alias(ColName.EXPANSION),
                pl.lit(event_type).alias(ColName.EVENT_TYPE),
                (pl.lit("Top") if player_cohort == "top" else pl.lit(None))
                .alias(ColName.PLAYER_COHORT)
                .cast(pl.String),
                *[val.alias(key) for key, val in deck_color_col_defs.items()],
            ]
        )
    )

    return df


def base_ratings_df(
    set_code: str,
    event_type: EventType = EventType.PREMIER,
    player_cohort: str = "all",
    deck_colors: str | list[str] = "any",
    *,
    start_date: dt.date,
    end_date: dt.date | None = None,
) -> pl.DataFrame:
    if end_date is None:
        end_date = dt.date.today() - dt.timedelta(days=1)

    if isinstance(deck_colors, str):
        deck_colors = [deck_colors]

    concat_list = []
    for i, deck_color in enumerate(deck_colors):
        ratings_dir, filename = cache.card_ratings_file_path(
            set_code,
            event_type,
            player_cohort,
            deck_color,
            start_date,
            end_date,
        )

        # rate-limit consecutive downloads, but not cache hits
        if i > 0 and not os.path.isfile(os.path.join(ratings_dir, filename)):
            sleep(5)

        user_group_param = (
            "" if player_cohort == "all" else f"&user_group={player_cohort}"
        )
        deck_color_param = "" if deck_color == "any" else f"&colors={deck_color}"

        url = RATINGS_TEMPLATE.format(
            set_code=set_code,
            format=event_type,
            user_group_param=user_group_param,
            deck_color_param=deck_color_param,
            start_date_str=start_date.strftime("%Y-%m-%d"),
            end_date_str=end_date.strftime("%Y-%m-%d"),
        )

        ratings_file_path = download_data_file(url, ratings_dir, filename)

        concat_list.append(
            pl.read_json(ratings_file_path, infer_schema_length=1000)
            .with_columns(
                (pl.lit(deck_color) if deck_color != "any" else pl.lit(None))
                .alias(ColName.MAIN_COLORS)
                .cast(pl.String)
            )
            .select(
                [
                    pl.lit(set_code).alias(ColName.EXPANSION),
                    pl.lit(event_type).alias(ColName.EVENT_TYPE),
                    (pl.lit("Top") if player_cohort == "top" else pl.lit(None))
                    .alias(ColName.PLAYER_COHORT)
                    .cast(pl.String),
                    ColName.MAIN_COLORS,
                    *[val.alias(key) for key, val in ratings_col_defs.items()],
                ]
            )
        )

    raw_df = pl.concat(concat_list)

    group_cols = [
        ColName.NAME,
        ColName.EXPANSION,
        ColName.MAIN_COLORS,
    ]

    attr_cols = [
        ColName.EVENT_TYPE,
        ColName.PLAYER_COHORT,
        ColName.COLOR,
        ColName.RARITY,
        ColName.IMAGE_URL,
    ]

    sum_cols = list(set(ratings_col_defs) - set(group_cols + attr_cols))

    attr_df = raw_df.select(group_cols + attr_cols).group_by(group_cols).first()
    sum_df = raw_df.select(group_cols + sum_cols).group_by(group_cols).sum()

    df = attr_df.join(sum_df, on=group_cols, join_nulls=True)

    return df
