import json
from datetime import datetime
from pyspark.sql.functions import explode, col

def collect_error_counts(invalid_df):
    """
    invalid_df contains validation_errors (array of strings). We'll count occurrences per message
    """
    if invalid_df is None or invalid_df.rdd.isEmpty():
        return {}
    # explode errors and count
    errors = invalid_df.select(explode(col("validation_errors")).alias("err"))
    counts = errors.groupBy("err").count().collect()
    return {row["err"]: row["count"] for row in counts}

def generate_report(valid_df, invalid_df, report_path):
    total = (valid_df.count() if valid_df is not None else 0) + (invalid_df.count() if invalid_df is not None else 0)
    valid_count = valid_df.count() if valid_df is not None else 0
    invalid_count = invalid_df.count() if invalid_df is not None else 0
    error_counts = collect_error_counts(invalid_df)

    report = {
        "run_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "records_loaded": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "error_types": error_counts
    }

    # write JSON locally (easy to read)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report
