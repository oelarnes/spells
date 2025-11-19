import datetime as dt
import os
import wget
from time import sleep

import polars as pl

from spells import cache
from spells.enums import ColName

RATINGS_TEMPLATE = (
    "https://www.17lands.com/card_ratings/data?expansion={set_code}&format={format}"
    "{user_group_param}{deck_color_param}&start_date={start_date_str}&end_date={end_date_str}"
)

DECK_COLOR_DATA_TEMPLATE = (
    "https://www.17lands.com/color_ratings/data?expansion={set_code}&event_type={format}"
    "{user_group_param}&start_date={start_date_str}&end_date={end_date_str}&combine_splash=true"
)

START_DATE_MAP = {
    "TLA": dt.date(2025, 11, 18),
    "PIO": dt.date(2024, 12, 10),
    "DFT": dt.date(2025, 2, 11),
    "TDM": dt.date(2025, 4, 8),
    "FIN": dt.date(2025, 6, 10),
    "EOE": dt.date(2025, 7, 29),
    "OM1": dt.date(2025, 9, 23),
    "Cube+-+Powered": dt.date(2025, 10, 28),
}

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


def deck_color_df(
    set_code: str,
    format: str = "PremierDraft",
    player_cohort: str = "all",
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
):
    if start_date is None:
        start_date = START_DATE_MAP[set_code]
    if end_date is None:
        end_date = dt.date.today() - dt.timedelta(days=1)

    target_dir, filename = cache.deck_color_file_path(
        set_code,
        format,
        player_cohort,
        start_date,
        end_date,
    )

    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    deck_color_file_path = os.path.join(target_dir, filename)

    if not os.path.isfile(deck_color_file_path):
        user_group_param = (
            "" if player_cohort == "all" else f"&user_group={player_cohort}"
        )

        url = DECK_COLOR_DATA_TEMPLATE.format(
            set_code=set_code,
            format=format,
            user_group_param=user_group_param,
            start_date_str=start_date.strftime("%Y-%m-%d"),
            end_date_str=end_date.strftime("%Y-%m-%d"),
        )

        wget.download(
            url,
            out=deck_color_file_path,
        )

    df = (
        pl.read_json(deck_color_file_path)
        .filter(~pl.col("is_summary"))
        .select(
            [
                pl.lit(set_code).alias(ColName.EXPANSION),
                pl.lit(format).alias(ColName.EVENT_TYPE),
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
    format: str = "PremierDraft",
    player_cohort: str = "all",
    deck_colors: str | list[str] = "any",
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pl.DataFrame:
    if start_date is None:
        start_date = START_DATE_MAP[set_code]
    if end_date is None:
        end_date = dt.date.today() - dt.timedelta(days=1)

    if isinstance(deck_colors, str):
        deck_colors = [deck_colors]

    concat_list = []
    for i, deck_color in enumerate(deck_colors):
        ratings_dir, filename = cache.card_ratings_file_path(
            set_code,
            format,
            player_cohort,
            deck_color,
            start_date,
            end_date,
        )

        if not os.path.isdir(ratings_dir):
            os.makedirs(ratings_dir)

        ratings_file_path = os.path.join(ratings_dir, filename)

        if not os.path.isfile(ratings_file_path):
            if i > 0:
                sleep(5)
            user_group_param = (
                "" if player_cohort == "all" else f"&user_group={player_cohort}"
            )
            deck_color_param = "" if deck_color == "any" else f"&colors={deck_color}"

            url = RATINGS_TEMPLATE.format(
                set_code=set_code,
                format=format,
                user_group_param=user_group_param,
                deck_color_param=deck_color_param,
                start_date_str=start_date.strftime("%Y-%m-%d"),
                end_date_str=end_date.strftime("%Y-%m-%d"),
            )

            wget.download(
                url,
                out=ratings_file_path,
            )

        concat_list.append(
            pl.read_json(ratings_file_path, infer_schema_length=540)
            .with_columns(
                (pl.lit(deck_color) if deck_color != "any" else pl.lit(None))
                .alias(ColName.MAIN_COLORS)
                .cast(pl.String)
            )
            .select(
                [
                    pl.lit(set_code).alias(ColName.EXPANSION),
                    pl.lit(format).alias(ColName.EVENT_TYPE),
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
