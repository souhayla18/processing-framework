# processing-framework
we will explain how to : 
        How to install dependencies

        How to run pipeline locally (python src/main.py)

        How to trigger Airflow DAG
Store rules, paths, sinks in configs/metadata.json
# Ominimo – Data Engineering Technical Test

## 🚀 Overview
This project implements a **metadata-driven PySpark pipeline** for ingesting, validating, and storing motor insurance policy data.

- Reads metadata from `configs/metadata.json`
- Loads JSON data from `data/input/`
- Validates fields dynamically
- Adds ingestion timestamp
- Separates valid vs invalid records
- Writes results to `data/output/ok/` and `data/output/ko/`
- Orchestrated with Airflow DAG (`dags/ominimo_pipeline_dag.py`)

---

## 🛠️ How to Run Locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
## 🐳 Run with Docker

1. Build the image:
   docker build -t ominimo-de-test .

2. Run the pipeline:
   docker run --rm -v $(pwd)/data:/app/data ominimo-de-test