from pyspark.sql.functions import col, when, lit

def validate(df, rules):
    error_exprs = []

    for field, rule in rules.items():
        if rule.get("required", False):
            df = df.withColumn(
                f"{field}_error",
                when(col(field).isNull(), lit(f"{field} is required"))
            )
        if rule.get("type") == "int":
            if "min" in rule:
                df = df.withColumn(
                    f"{field}_error",
                    when((col(field) < rule["min"]) | col(field).isNull(), lit(f"{field} below min"))
                )
            if "max" in rule:
                df = df.withColumn(
                    f"{field}_error",
                    when((col(field) > rule["max"]) | col(field).isNull(), lit(f"{field} above max"))
                )
        if rule.get("not_empty", False):
            df = df.withColumn(
                f"{field}_error",
                when((col(field) == "") | col(field).isNull(), lit(f"{field} is empty"))
            )
        error_exprs.append(f"{field}_error")

    valid_df = df.filter(" AND ".join([f"{c} IS NULL" for c in error_exprs]))
    invalid_df = df.filter(" OR ".join([f"{c} IS NOT NULL" for c in error_exprs]))
    return valid_df, invalid_df
