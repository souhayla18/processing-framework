from pyspark.sql.functions import (
    col, when, lit, array, array_remove, size, struct, map_from_entries, map_keys
)
from pyspark.sql import DataFrame

def _apply_rule_expr(df: DataFrame, field: str, rule: dict, idx: int):
    rule_type = rule.get("rule")
    col_name = f"{field}_error_{idx}"

    if rule_type == "required":
        df = df.withColumn(col_name, when(col(field).isNull(), lit(f"{field} is missing")))
    elif rule_type == "not_empty":
        df = df.withColumn(col_name, when((col(field) == "") | col(field).isNull(), lit(f"{field} is empty")))
    elif rule_type == "type":
        t = rule.get("type")
        if t == "int":
            # Accept numeric strings and integers; treat null as error if required covers it
            df = df.withColumn(col_name, when(~col(field).cast("string").rlike(r"^\d+$"), lit(f"{field} not int")))
        else:
            df = df.withColumn(col_name, lit(None))
    elif rule_type == "between":
        mn = rule.get("min")
        mx = rule.get("max")
        df = df.withColumn(col_name,
                           when((col(field).cast("double") < lit(mn)) | (col(field).cast("double") > lit(mx)) | col(field).isNull(),
                                lit(f"{field} not in [{mn},{mx}]")))
    elif rule_type == "regex":
        pattern = rule.get("pattern")
        df = df.withColumn(col_name, when((col(field).isNotNull()) & (~col(field).rlike(pattern)),
                                         lit(f"{field} not match {pattern}")))
    else:
        df = df.withColumn(col_name, lit(None))

    return df, col_name

def validate(df: DataFrame, rules: list):
    """
    rules: list of dicts e.g.
      [{"field":"driver_age","rule":"between","min":18,"max":100}, ...]
    Returns: (valid_df, invalid_df)
      - invalid_df contains a map column "validation_errors" (string->string)
      - valid_df has validation_errors = null
    """
    # ensure fields exist
    for r in rules:
        f = r["field"]
        if f not in df.columns:
            df = df.withColumn(f, lit(None))

    error_cols = []
    for idx, rule in enumerate(rules):
        field = rule["field"]
        df, errcol = _apply_rule_expr(df, field, rule, idx)
        error_cols.append(errcol)

    # Build array of structs for non-null errors, then map_from_entries
    if error_cols:
        error_structs = [ when(col(c).isNotNull(), struct(lit(c.split("_error_")[0]).alias("k"), col(c).alias("v"))) for c in error_cols ]
        df = df.withColumn("validation_errors_arr", array(*error_structs))
        # remove null entries from the array
        df = df.withColumn("validation_errors_arr", array_remove(col("validation_errors_arr"), None))
        # convert to map (k->v)
        df = df.withColumn("validation_errors", map_from_entries(col("validation_errors_arr")))
    else:
        df = df.withColumn("validation_errors", lit(None))

    # Split valid vs invalid
    valid_df = df.filter((col("validation_errors").isNull()) | (size(map_keys(col("validation_errors"))) == 0))
    invalid_df = df.filter((col("validation_errors").isNotNull()) & (size(map_keys(col("validation_errors"))) > 0))

    # drop temp error columns and the array (keep validation_errors)
    drop_candidates = error_cols + ["validation_errors_arr"]
    drop_now = [c for c in drop_candidates if c in valid_df.columns]
    if drop_now:
        valid_df = valid_df.drop(*drop_now)
    drop_now = [c for c in drop_candidates if c in invalid_df.columns]
    if drop_now:
        invalid_df = invalid_df.drop(*drop_now)

    return valid_df, invalid_df
