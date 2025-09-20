# processing-framework
Metadata-driven PySpark pipeline for motor insurance policy ingestion, validation, transformation, and reporting.
Designed for Ominimo DE technical test.

## What it does
- Reads motor insurance JSON lines as defined in `configs/metadata.json`
- Applies field-level, metadata-driven validations
- Adds ingestion metadata and transformation features (standardize plate, age buckets, run_id)
- Separates valid vs invalid records (written to `data/output/ok/` and `data/output/ko/`)
- Generates a data quality report `data/output/report.json`
- Optional Airflow DAG for event-driven orchestration
- Dockerfile included for reproducibility

## Project structure
processing-framework/
├── configs/metadata.json
├── dags/ominimo_pipeline_dag.py
├── data/
│ ├── input/motor_policy.json
│ └── output/
├── src/
│ ├── main.py
│ ├── config_parser.py
│ ├── data_loader.py
│ ├── validator.py
│ ├── transformer.py
│ ├── writer.py
│ ├── reporter.py
│ └── utils.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md

sql
Copy code

## Quick start (local Python)
1. Create virtual env and install:
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

markdown
Copy code
2. Run pipeline:
python src/main.py

markdown
Copy code
3. Results:
- Valid records: `data/output/ok/` (JSON files)
- Invalid records: `data/output/ko/` (JSON files)
- Report: `data/output/report.json`

## Run with Docker
1. Build:
docker build -t processing-framework .

css
Copy code
2. Run (mount data folder to keep outputs):
docker run --rm -v $(pwd)/data:/app/data processing-framework

markdown
Copy code

## Run with Airflow (optional)
1. `docker-compose up` (requires docker-compose)
2. Open Airflow UI: `http://localhost:8080`
3. Trigger DAG `ominimo_pipeline`

## How to extend
- Add rules to `configs/metadata.json` under `validations`
- Toggle transformations in `transformations`
- Add new sources / formats in `dataflows[].sources`
- For production, point sinks to cloud storage (S3, GCS) or a data lake, and update writer logic

## Notes
- The solution is metadata-driven: adding new validations or sources should not require changing pipeline code.
- Docker helps reproducibility; Airflow demonstrates orchestration.