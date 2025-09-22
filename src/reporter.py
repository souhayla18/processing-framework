from datetime import datetime
import json
from pyspark.sql.functions import explode, col

def generate_report(valid_df, invalid_df, report_path):
    valid_count = valid_df.count() if valid_df else 0
    invalid_count = invalid_df.count() if invalid_df else 0
    total = valid_count + invalid_count

    error_types = {}
    if invalid_df is not None and "validation_errors" in invalid_df.columns:
        # flatten errors
        exploded = invalid_df.select(explode(col("validation_errors")).alias("error"))
        # count each type
        error_counts = exploded.groupBy("error").count().collect()
        for row in error_counts:
            error_types[row["error"]] = row["count"]

    report = {
        "run_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "records_loaded": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "error_types": error_types
    }

    # write JSON
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report
