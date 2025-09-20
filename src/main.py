import logging
from config_parser import load_metadata
from data_loader import load_data
from validator import validate
from transformer import apply_transformations
from writer import write_data
from reporter import generate_report
from utils import get_spark
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("processing-framework")

def main(config_path="configs/metadata.json"):
    spark = get_spark()
    try:
        meta = load_metadata(config_path)
        flow = meta["dataflows"][0]
        src = flow["sources"][0]

        logger.info("Loading data...")
        df = load_data(spark, src)
        logger.info(f"Loaded {df.count()} records")

        logger.info("Validating data...")
        valid_df, invalid_df = validate(df, flow.get("validations", []))
        logger.info(f"Validation results: valid={valid_df.count()}, invalid={invalid_df.count()}")

        logger.info("Applying transformations...")
        valid_df, invalid_df = apply_transformations(valid_df, invalid_df, flow.get("transformations", {}))

        # ensure output directories exist on host (for local runs, ensure path exist)
        ok_path = flow["sinks"]["ok"]
        ko_path = flow["sinks"]["ko"]
        os.makedirs(ok_path, exist_ok=True)
        os.makedirs(ko_path, exist_ok=True)

        logger.info("Writing valid records...")
        write_data(valid_df, ok_path, format="json", mode="overwrite")
        logger.info("Writing invalid records...")
        write_data(invalid_df, ko_path, format="json", mode="overwrite")

        # generate data quality report
        report_path = flow["sinks"].get("report", "data/output/report.json")
        logger.info("Generating report...")
        report = generate_report(valid_df, invalid_df, report_path)
        logger.info(f"Report generated: {report_path} -> {report}")

    except Exception as e:
        logger.exception("Pipeline failed")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    cfg = sys.argv[1] if len(sys.argv) > 1 else "configs/metadata.json"
    main(cfg)
