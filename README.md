# processing-framework
Metadata-driven PySpark pipeline for motor insurance policy ingestion, validation, transformation, and reporting.

## What it does
- Reads motor insurance JSON lines as defined in `configs/metadata.json`
- Applies field-level, metadata-driven validations
- Adds ingestion metadata and transformation features (standardize plate, age buckets, run_id…)
- Separates valid vs invalid records (written to `data/output/ok/` and `data/output/ko/`)
- Generates a data quality report `data/output/report.json`
- Optional Airflow DAG for event-driven orchestration
- Dockerfile included for reproducibility

## Project structure
```text
processing-framework/
├── configs/
│   └── metadata.json
├── dags/
│   └── pipeline_dag.py
├── data/
│   ├── input/
│   │   └── motor_policy.json
│   └── output/
├── src/
│   ├── main.py
│   ├── config_parser.py
│   ├── data_loader.py
│   ├── validator.py
│   ├── transformer.py
│   ├── writer.py
│   ├── reporter.py
│   └── utils.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```
## Quick start (local Python)
Create virtual environment and install dependencies:

```
python -m venv .venv
source .venv/bin/activate   # On Linux/Mac
.\.venv\Scripts\Activate.ps1  # On Windows PowerShell
pip install -r requirements.txt
```
Run pipeline:

```
python src/main.py
```
Results:

Valid records: data/output/ok/ (JSON files)

Invalid records: data/output/ko/ (JSON files)

Report: data/output/report.json

## Run with Docker
Build image:

```
docker build -t processing-framework .
```
Run container (mount data folder to keep outputs):
```
docker run --rm -v $(pwd)/data:/app/data processing-framework
```
## Run with Airflow (optional)
Start services:

```
docker-compose up
```
Open Airflow UI: http://localhost:8080

Trigger DAG: ipeline

## How to extend
Add rules to configs/metadata.json under validations

Toggle transformations in transformations

Add new sources / formats in dataflows[].sources

For production, point sinks to cloud storage (S3, GCS) or a data lake, and update writer logic
