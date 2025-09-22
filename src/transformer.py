# src/transformer.py
from datetime import datetime
from pyspark.sql.functions import (
    trim, upper, col, when, lit, sha2, concat_ws, input_file_name,
    size, map_keys, coalesce
)
from pyspark.sql import DataFrame

def _standardize_plate(df: DataFrame, plate_col: str = "plate_number") -> DataFrame:
    if plate_col in df.columns:
        return df.withColumn(plate_col, upper(trim(col(plate_col))))
    return df

def _add_age_bucket(df: DataFrame, age_col: str = "driver_age", new_col: str = "driver_age_bucket") -> DataFrame:
    if age_col in df.columns:
        return df.withColumn(
            new_col,
            when(col(age_col) < 25, "<25")
            .when((col(age_col) >= 25) & (col(age_col) <= 40), "25-40")
            .when((col(age_col) > 40) & (col(age_col) <= 60), "41-60")
            .when(col(age_col) > 60, "60+")
            .otherwise("unknown")
        )
    return df

def _add_lineage(df: DataFrame, source_col: str = "source_file") -> DataFrame:
    return df.withColumn(source_col, input_file_name())

def _add_ingestion_id(df: DataFrame, id_col: str = "ingestion_id",
                      policy_col: str = "policy_number", ingestion_col: str = "ingestion_dt") -> DataFrame:
    if policy_col in df.columns and ingestion_col in df.columns:
        return df.withColumn(
            id_col,
            sha2(
                concat_ws("||",
                          coalesce(col(policy_col), lit("")),
                          col(ingestion_col).cast("string"),
                          coalesce(col("source_file"), lit(""))), 256
            )
        )
    return df

def apply_transformations(valid_df: DataFrame, invalid_df: DataFrame,
                          transformations: dict):
    # generate single ingestion timestamp for the run
    ingestion_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if transformations.get("add_ingestion_dt", False):
        valid_df = valid_df.withColumn("ingestion_dt", lit(ingestion_ts))
        invalid_df = invalid_df.withColumn("ingestion_dt", lit(ingestion_ts))

    if transformations.get("add_lineage", False):
        valid_df = _add_lineage(valid_df)
        invalid_df = _add_lineage(invalid_df)

    if transformations.get("add_ingestion_id", False):
        valid_df = _add_ingestion_id(valid_df)
        invalid_df = _add_ingestion_id(invalid_df)
    
    if transformations.get("standardize_plate", False):
        valid_df = _standardize_plate(valid_df)


    if transformations.get("add_age_bucket", False):
        valid_df = _add_age_bucket(valid_df)

    return valid_df, invalid_df
