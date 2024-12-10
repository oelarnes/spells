import polars as pl

from spells.enums import ColType
from spells.columns import ColSpec

def attr_metrics(attr):
    return {
        f"seen_{attr}": ColSpec(
            col_type=ColType.NAME_SUM,
            expr=(lambda name, card_context: pl.when(pl.col(f"pack_card_{name}") > 0)
                .then(card_context[name][attr])
                .otherwise(None)),
        ),
        f"pick_{attr}": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=lambda name, card_context: card_context[name][attr]
        ),
        f"least_{attr}_taken": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=(lambda names: pl.col(f'pick_{attr}')
                <= pl.min_horizontal([pl.col(f"seen_{attr}_{name}") for name in names])),
        ),
        f"least_{attr}_taken_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"least_{attr}_taken") / pl.col("num_taken"), 
        ),
        f"greatest_{attr}_taken": ColSpec(
            col_type=ColType.PICK_SUM,
            expr=(lambda names: pl.col(f'pick_{attr}')
                >= pl.max_horizontal([pl.col(f"seen_{attr}_{name}") for name in names])),
        ),
        f"greatest_{attr}_taken_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"greatest_{attr}_taken") / pl.col("num_taken"), 
        ),
        f"pick_{attr}_mean": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col(f"pick_{attr}") / pl.col("num_taken")
        )
    }
