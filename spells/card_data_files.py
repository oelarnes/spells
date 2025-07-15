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
    "DFT": dt.date(2025, 2, 11),
    "TDM": dt.date(2025, 4, 8),
    "FIN": dt.date(2025, 6, 10),
}

ratings_col_defs = {
    ColName.NAME: pl.col("name"),
    ColName.COLOR: pl.col("color"),
    ColName.RARITY: pl.col("rarity"),
    ColName.CARD_TYPE: pl.col("types"),
    ColName.IMAGE_URL: pl.col("url"),
    ColName.NUM_SEEN: pl.col("seen_count"),
    ColName.LAST_SEEN: pl.col("seen_count") * pl.col("avg_seen"),
    ColName.NUM_TAKEN: pl.col("pick_count"),
    ColName.TAKEN_AT: pl.col("pick_count") * pl.col("avg_pick"),
    ColName.DECK: pl.col("game_count"),
    ColName.WON_DECK: pl.col("win_rate") * pl.col("game_count"),
    ColName.SIDEBOARD: pl.col("pool_count") - pl.col("game_count"),
    ColName.OPENING_HAND: pl.col("opening_hand_game_count"),
    ColName.WON_OPENING_HAND: pl.col("opening_hand_game_count")
    * pl.col("opening_hand_win_rate"),
    ColName.DRAWN: pl.col("drawn_game_count"),
    ColName.WON_DRAWN: pl.col("drawn_win_rate") * pl.col("drawn_game_count"),
    ColName.NUM_GIH: pl.col("ever_drawn_game_count"),
    ColName.NUM_GIH_WON: pl.col("ever_drawn_game_count")
    * pl.col("ever_drawn_win_rate"),
    ColName.NUM_GNS: pl.col("never_drawn_game_count"),
    ColName.WON_NUM_GNS: pl.col("never_drawn_game_count")
    * pl.col("never_drawn_win_rate"),
}

deck_color_col_defs = {
    ColName.MAIN_COLORS: pl.col("short_name"),
    ColName.NUM_GAMES: pl.col("games"),
    ColName.NUM_WON: pl.col("wins"),
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
                (pl.lit("Top") if player_cohort == "top" else pl.lit(None)).alias(
                    ColName.PLAYER_COHORT
                ),
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
            deck_color_param = "" if deck_color == "any" else f"&deck_colors={deck_color}"

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

        concat_list.append(pl.read_json(ratings_file_path).with_columns(
            (pl.lit(deck_color) if deck_color != "any" else pl.lit(None)).alias(
                ColName.MAIN_COLORS
            )
        ))
    df = pl.concat(concat_list)

    return df.select(
        [
            pl.lit(set_code).alias(ColName.EXPANSION),
            pl.lit(format).alias(ColName.EVENT_TYPE),
            (pl.lit("Top") if player_cohort == "top" else pl.lit(None)).alias(
                ColName.PLAYER_COHORT
            ),
            ColName.MAIN_COLORS,
            *[val.alias(key) for key, val in ratings_col_defs.items()],
        ]
    )
