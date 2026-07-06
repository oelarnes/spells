import datetime as dt
import json
import os
from pathlib import Path
import wget
from time import sleep

import polars as pl

from spells import cache
from spells.enums import ColName, EventType, TimePeriod

RATINGS_TEMPLATE = (
    "https://www.17lands.com/api/card_data?expansion={set_code}&event_type={event_type}"
    "&time_period={time_period}{user_group_param}{deck_color_param}"
)

DECK_COLOR_DATA_TEMPLATE = (
    "https://www.17lands.com/color_ratings/data?expansion={set_code}&event_type={event_type}"
    "&time_period={time_period}{user_group_param}&combine_splash=true"
)


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


def _validated_as_of(as_of: dt.date | None) -> dt.date:
    today = dt.date.today()
    if as_of is None:
        return today
    if as_of > today:
        raise ValueError(f"as_of={as_of} is in the future")
    return as_of


def _fetch_snapshot(url: str, target_dir: str, filename: str, as_of: dt.date) -> list:
    """Return the cached JSON payload for an `as_of` snapshot, downloading it only
    when `as_of` is today. 17lands resolves `time_period` relative to its own
    current date, so a missed snapshot for a past `as_of` cannot be reconstructed.
    An empty payload is not cached: it indicates an unsupported query (bad
    set/event_type, or a filter combination the 17lands precompute doesn't cover).
    """
    file_path = os.path.join(target_dir, filename)

    if os.path.isfile(file_path):
        return json.loads(Path(file_path).read_text())

    if as_of < dt.date.today():
        raise ValueError(
            f"No cached snapshot {filename} for as_of={as_of}, and past snapshots "
            "cannot be fetched"
        )

    download_data_file(url, target_dir, filename)
    payload = json.loads(Path(file_path).read_text())
    if not payload:
        os.remove(file_path)
    return payload


def deck_color_df(
    set_code: str,
    event_type: EventType = EventType.PREMIER,
    player_cohort: str = "all",
    *,
    time_period: TimePeriod = TimePeriod.ALL_TIME,
    as_of: dt.date | None = None,
):
    time_period = TimePeriod(time_period)
    as_of = _validated_as_of(as_of)

    target_dir, filename = cache.deck_color_file_path(
        set_code,
        event_type,
        player_cohort,
        time_period,
        as_of,
    )

    user_group_param = (
        "" if player_cohort == "all" else f"&user_group={player_cohort}"
    )

    url = DECK_COLOR_DATA_TEMPLATE.format(
        set_code=set_code,
        event_type=event_type,
        time_period=time_period,
        user_group_param=user_group_param,
    )

    payload = _fetch_snapshot(url, target_dir, filename, as_of)
    if not payload:
        raise ValueError(
            f"Empty color ratings response for {set_code} {event_type} "
            f"time_period={time_period} player_cohort={player_cohort}"
        )

    df = (
        pl.from_dicts(payload)
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
    time_period: TimePeriod = TimePeriod.ALL_TIME,
    as_of: dt.date | None = None,
) -> pl.DataFrame:
    time_period = TimePeriod(time_period)
    as_of = _validated_as_of(as_of)

    if isinstance(deck_colors, str):
        deck_colors = [deck_colors]

    concat_list = []
    for i, deck_color in enumerate(deck_colors):
        ratings_dir, filename = cache.card_ratings_file_path(
            set_code,
            event_type,
            player_cohort,
            deck_color,
            time_period,
            as_of,
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
            event_type=event_type,
            time_period=time_period,
            user_group_param=user_group_param,
            deck_color_param=deck_color_param,
        )

        payload = _fetch_snapshot(url, ratings_dir, filename, as_of)
        if not payload:
            gap_hint = (
                " (17lands does not precompute user_group and colors together)"
                if player_cohort != "all" and deck_color != "any"
                else ""
            )
            raise ValueError(
                f"Empty card ratings response for {set_code} {event_type} "
                f"time_period={time_period} player_cohort={player_cohort} "
                f"colors={deck_color}{gap_hint}"
            )

        concat_list.append(
            pl.from_dicts(payload, infer_schema_length=1000)
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
