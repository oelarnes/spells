from typing import Callable

import math

import polars as pl

from spells.enums import ColType, ColName
from spells.columns import ColSpec
from spells.cache import spells_print


def print_ext(ext: dict[str, ColSpec]) -> None:
    spells_print("create", "Created extensions:")
    for key in ext:
        print("\t" + key)


def seen_greatest_name_fn(attr: str) -> Callable:
    def inner(names: list[str]) -> pl.Expr:
        expr = pl.lit(None)
        for name in names:
            expr = (
                pl.when(pl.col(f"seen_{attr}_is_greatest_{name}"))
                .then(pl.lit(name))
                .otherwise(expr)
            )
        return expr

    return inner


def context_cols(attr, silent: bool = True) -> dict[str, ColSpec]:
    ext = {
        f"seen_{attr}": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=(
                lambda name, card_context: pl.lit(None)
                if card_context[name].get(attr) is None
                or math.isnan(card_context[name][attr])
                else pl.when(pl.col(f"pack_card_{name}") > 0)
                .then(card_context[name][attr])
                .otherwise(None)
            ),
        ),
        f"pick_{attr}_sum": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda name, card_context: pl.lit(None)
            if card_context[name].get(attr) is None
            or math.isnan(card_context[name][attr])
            else card_context[name][attr],
        ),
        f"pick_{attr}": ColSpec(
            col_type=ColType.GROUP_BY, expr=pl.col(f"pick_{attr}_sum")
        ),
        f"pool_{attr}": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=(
                lambda name, card_context: pl.lit(None)
                if card_context[name].get(attr) is None
                or math.isnan(card_context[name][attr])
                else card_context[name][attr] * pl.col(f"pool_{name}")
            ),
        ),
        f"pool_{attr}_sum": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda names: pl.sum_horizontal(
                [pl.col(f"pool_{attr}_{name}") for name in names]
            ),
        ),
        f"pool_pick_{attr}_sum": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=pl.col(f"pick_{attr}_sum") + pl.col(f"pool_{attr}_sum"),
        ),
        f"seen_{attr}_is_greatest": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=lambda name: pl.col(f"seen_{attr}_{name}")
            == pl.col(f"greatest_{attr}_seen"),
        ),
        f"seen_greatest_{attr}_name": ColSpec(
            col_type=ColType.GROUP_BY, expr=seen_greatest_name_fn(attr)
        ),
        f"seen_{attr}_greater": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=lambda name: pl.col(f"seen_{attr}_{name}")
            > pl.col(f"pick_{attr}_sum"),
        ),
        f"seen_{attr}_less": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=lambda name: pl.col(f"seen_{attr}_{name}")
            < pl.col(f"pick_{attr}_sum"),
        ),
        f"greatest_{attr}_seen": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda names: pl.max_horizontal(
                [pl.col(f"seen_{attr}_{name}") for name in names]
            ),
        ),
        f"seen_{attr}_pack_sum": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda names: pl.sum_horizontal(
                [pl.col(f"seen_{attr}_{name}") for name in names]
            ),
        ),
        f"least_{attr}_seen": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda names: pl.min_horizontal(
                [pl.col(f"seen_{attr}_{name}") for name in names]
            ),
        ),
        f"pick_{attr}_rank_greatest": ColSpec(
            col_type=ColType.GROUP_BY,
            expr=lambda names: pl.sum_horizontal(
                [pl.col(f"seen_{attr}_greater_{name}") for name in names]
            )
            + 1,
        ),
        f"pick_{attr}_rank_least": ColSpec(
            col_type=ColType.GROUP_BY,
            expr=lambda names: pl.sum_horizontal(
                [pl.col(f"seen_{attr}_less_{name}") for name in names]
            )
            + 1,
        ),
        f"pick_{attr}_rank_greatest_sum": ColSpec(
            col_type=ColType.PICK_SUM, expr=pl.col(f"pick_{attr}_rank_greatest")
        ),
        f"pick_{attr}_rank_least_sum": ColSpec(
            col_type=ColType.PICK_SUM, expr=pl.col(f"pick_{attr}_rank_least")
        ),
        f"pick_{attr}_vs_least": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=pl.col(f"pick_{attr}_sum") - pl.col(f"least_{attr}_seen"),
        ),
        f"pick_{attr}_vs_greatest": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=pl.col(f"pick_{attr}_sum") - pl.col(f"greatest_{attr}_seen"),
        ),
        f"pick_{attr}_vs_least_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"pick_{attr}_vs_least") / pl.col(ColName.NUM_TAKEN),
        ),
        f"pick_{attr}_vs_greatest_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"pick_{attr}_vs_greatest") / pl.col(ColName.NUM_TAKEN),
        ),
        f"least_{attr}_taken": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=pl.col(f"pick_{attr}_sum") <= pl.col(f"least_{attr}_seen"),
        ),
        f"least_{attr}_taken_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"least_{attr}_taken") / pl.col(ColName.NUM_TAKEN),
        ),
        f"greatest_{attr}_taken": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=pl.col(f"pick_{attr}_sum") >= pl.col(f"greatest_{attr}_seen"),
        ),
        f"greatest_{attr}_taken_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"greatest_{attr}_taken") / pl.col(ColName.NUM_TAKEN),
        ),
        f"pick_{attr}_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"pick_{attr}_sum") / pl.col(ColName.NUM_TAKEN),
        ),
    }

    if not silent:
        print_ext(ext)

    return ext


def stat_cols(attr: str, silent: bool = True) -> dict[str, ColSpec]:
    ext = {
        f"{attr}_deck_weight_group": ColSpec(
            col_type=ColType.AGG, expr=pl.col(f"{attr}") * pl.col(ColName.DECK)
        ),
        f"{attr}_deck_weight_total": ColSpec(
            col_type=ColType.AGG, expr=pl.col(f"{attr}_deck_weight_group").sum()
        ),
        f"{attr}_dw_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_deck_weight_total") / pl.col(ColName.DECK_TOTAL),
        ),
        f"{attr}_dw_excess": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}") - pl.col(f"{attr}_dw_mean"),
        ),
        f"{attr}_dw_var": ColSpec(
            col_type=ColType.AGG,
            expr=(pl.col(f"{attr}_dw_excess").pow(2) * pl.col(ColName.DECK)).sum()
            / pl.col(ColName.DECK_TOTAL),
        ),
        f"{attr}_dw_stdev": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_dw_var").sqrt(),
        ),
        f"{attr}_dwz": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_dw_excess") / pl.col(f"{attr}_dw_stdev"),
        ),
        f"{attr}_pool_weight_group": ColSpec(
            col_type=ColType.AGG, expr=pl.col(f"{attr}") * pl.col(ColName.NUM_IN_POOL)
        ),
        f"{attr}_pool_weight_total": ColSpec(
            col_type=ColType.AGG, expr=pl.col(f"{attr}_pool_weight_group").sum()
        ),
        f"{attr}_pw_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_pool_weight_total")
            / pl.col(ColName.NUM_IN_POOL_TOTAL),
        ),
        f"{attr}_pw_excess": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}") - pl.col(f"{attr}_pw_mean"),
        ),
        f"{attr}_pw_var": ColSpec(
            col_type=ColType.AGG,
            expr=(
                pl.col(f"{attr}_pw_excess").pow(2) * pl.col(ColName.NUM_IN_POOL)
            ).sum()
            / pl.col(ColName.NUM_IN_POOL_TOTAL),
        ),
        f"{attr}_pw_stdev": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_pw_var").sqrt(),
        ),
        f"{attr}_pwz": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"{attr}_pw_excess") / pl.col(f"{attr}_pw_stdev"),
        ),
    }

    if not silent:
        print_ext(ext)

    return ext
