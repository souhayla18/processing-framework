import logging
from config_parser import load_metadata
from data_loader import load_data
from validator import validate
from transformer import apply_transformations
from writer import write_data
from utils import get_spark

logging.basicConfig(level=logging.INFO)

def main():
    spark = get_spark()
    metadata = load_metadata("configs/metadata.json")["dataflows"][0]

    # Load
    df = load_data(spark, metadata["sources"][0])
    logging.info(f"Loaded {df.count()} records")

    # Validate
    valid_df, invalid_df = validate(df, metadata["validations"])
    logging.info(f"Valid: {valid_df.count()} | Invalid: {invalid_df.count()}")

    # Transform
    valid_df, invalid_df = apply_transformations(valid_df, invalid_df, metadata["transformations"])

    # Write
    write_data(valid_df, metadata["sinks"]["ok"])
    write_data(invalid_df, metadata["sinks"]["ko"])

    spark.stop()

if __name__ == "__main__":
    main()
