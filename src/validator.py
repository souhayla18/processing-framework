from pyspark.sql.functions import (
    col, when, lit, array, size, expr
)
from pyspark.sql import DataFrame


def _apply_rule_expr(df: DataFrame, field: str, rule: dict, idx: int):
    rule_type = rule.get("rule")
    col_name = f"{field}_{rule_type}_{idx}_error"

    if rule_type == "required":
        df = df.withColumn(
            col_name,
            when(col(field).isNull(), lit(f"{field} is missing"))
        )

    elif rule_type == "not_empty":
        df = df.withColumn(
            col_name,
            when(col(field).isNull() | (col(field) == ""), lit(f"{field} is empty"))
        )

    elif rule_type == "type":
        t = rule.get("type")
        if t == "int":
            df = df.withColumn(
                col_name,
                when(
                    col(field).isNotNull() & ~col(field).cast("string").rlike(r"^\d+$"),
                    lit(f"{field} not int")
                )
            )
        else:
            df = df.withColumn(col_name, lit(None))

    elif rule_type == "between":
        mn = rule.get("min")
        mx = rule.get("max")
        df = df.withColumn(
            col_name,
            when(
                col(field).isNotNull() &
                ((col(field).cast("double") < lit(mn)) | (col(field).cast("double") > lit(mx))),
                lit(f"{field} not in [{mn},{mx}]")
            )
        )

    elif rule_type == "regex":
        pattern = rule.get("pattern")
        df = df.withColumn(
            col_name,
            when(col(field).isNotNull() & ~col(field).rlike(pattern),
                 lit(f"{field} not match {pattern}"))
        )

    else:
        df = df.withColumn(col_name, lit(None))

    return df, col_name


def validate(df: DataFrame, rules: list):
    # Ensure all referenced fields exist
    for r in rules:
        f = r["field"]
        if f not in df.columns:
            df = df.withColumn(f, lit(None))

    error_cols = []
    for idx, rule in enumerate(rules):
        field = rule["field"]
        df, errcol = _apply_rule_expr(df, field, rule, idx)
        error_cols.append(errcol)

    # Collect all non-null errors
    df = df.withColumn("validation_errors", array(*[col(c) for c in error_cols]))
    df = df.withColumn("validation_errors", expr("filter(validation_errors, x -> x is not null)"))

    valid_df = df.filter(size(col("validation_errors")) == 0)
    invalid_df = df.filter(size(col("validation_errors")) > 0)

    # Drop temp error columns
    drop_candidates = error_cols
    valid_df = valid_df.drop(*drop_candidates)
    invalid_df = invalid_df.drop(*drop_candidates)

    return valid_df, invalid_df
