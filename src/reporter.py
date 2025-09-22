from datetime import datetime
import json

def generate_report(valid_df, invalid_df, report_path):
    # safeguard counts
    valid_count = valid_df.count() if valid_df is not None else 0
    invalid_count = invalid_df.count() if invalid_df is not None else 0
    total = valid_count + invalid_count
    
    # ensure invalid_df has 'validation_errors' column before collecting
    ##if invalid_df is not None and 'validation_errors' not in invalid_df.columns:
   #     from pyspark.sql.functions import lit, array
   #     invalid_df = invalid_df.withColumn("validation_errors", array(lit("unknown_error")))
    
    #error_counts = collect_error_counts(invalid_df)

    report = {
        "run_date": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "records_loaded": total,
        "valid": valid_count,
        "invalid": invalid_count
    }

    # write JSON locally (easy to read)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return report
