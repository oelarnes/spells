import polars as pl

def convert_to_expr_list(
    input: str | pl.Expr | list[str | pl.Expr] | None
):
    if input is None:
        return []

    input_list = [input] if isinstance(input, str | pl.Expr) else input
    return [pl.col(i) if isinstance(i, str) else i for i in input_list]


def wavg(
    df: pl.DataFrame, 
    cols: str | pl.Expr | list[str | pl.Expr],
    weights: str | pl.Expr | list[str | pl.Expr],
    group_by: str | pl.Expr | list[str | pl.Expr] | None = None,
    new_names: str | list[str] | None = None,
) -> pl.DataFrame:
    col_list = convert_to_expr_list(cols)
    weight_list = convert_to_expr_list(weights)
    gbs = convert_to_expr_list(group_by)

    name_list: list[str]
    if isinstance(new_names, str):
        name_list = [new_names]
    elif new_names is None:
        name_list = [c.meta.output_name() for c in col_list]
    else:
        name_list = list(new_names)

    assert len(name_list) == len(col_list), f"{len(name_list)} names provided for {len(col_list)} columns"
    assert len(weight_list) == len(col_list), f"{len(weight_list)} weights provided for {len(col_list)} columns" 
    
    to_group = df.select(gbs + weight_list + [ 
        (c * weight_list[i]) for i, c in enumerate(col_list)
    ]) 

    grouped = to_group if not gbs else to_group.group_by(gbs)

    return grouped.sum().select(
        gbs + 
        [pl.col(c.meta.output_name()).alias(name_list[i]) for i, c in enumerate(col_list)]
    )


