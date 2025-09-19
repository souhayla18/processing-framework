from pyspark.sql.functions import current_timestamp

def apply_transformations(valid_df, invalid_df, transformations):
    if transformations.get("add_ingestion_dt", False):
        valid_df = valid_df.withColumn("ingestion_dt", current_timestamp())
        invalid_df = invalid_df.withColumn("ingestion_dt", current_timestamp())
    return valid_df, invalid_df
