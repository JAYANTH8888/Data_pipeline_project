
# Data & AI Engineering & CI/CD Pipeline 🧊

## Overview
This project demonstrates a scalable, automated data pipeline using Apache Iceberg, PySpark, and MLflow, managed by a CI/CD pipeline (GitHub Actions). It ingests, deduplicates, and harmonizes corporate data from multiple sources into an Iceberg table, trains a machine learning model, and registers the model artifact.


### Architecture
- **Data Ingestion & Harmonization:**
  - Reads from two simulated corporate data sources:
    - Source 1: Supply Chain Data (`sample_data/source1_supply_chain.csv`)
    - Source 2: Financial Data (`sample_data/source2_financial.csv`)
  - Entity resolution is performed using a heuristic (cleaned corporate name + normalized address) to assign a unique corporate ID and merge records.
  - Harmonized data is upserted into an Apache Iceberg table (`corporate_registry`) using transactional SQL (`MERGE INTO`).
- **ML Pipeline:**
  - Reads harmonized data from the Iceberg table.
  - Performs feature engineering (e.g., predicting if profit is above a threshold).
  - Trains a logistic regression model using PySpark ML.
  - Model is tracked and registered with MLflow.
- **CI/CD:**
  - GitHub Actions runs unit tests (including entity resolution), schema checks, ETL, and ML scripts on push/PR.
  - Model artifacts are uploaded as build artifacts.

## Setup Instructions

### 1. Iceberg Metastore Setup
- Uses local Hadoop catalog for demonstration (see `etl/etl_iceberg.py`).
- For production, configure AWS Glue, GCP BigLake, or REST catalog as needed.


### 2. Running Locally
- Install dependencies:
  ```bash
  pip install pyspark mlflow pyiceberg pytest
  ```
- Run ETL (entity resolution & harmonization):
  ```bash
  python etl/etl_iceberg.py
  ```
- Run ML training:
  ```bash
  python ml/ml_train.py
  ```
- Run tests:
  ```bash
  PYTHONPATH=. pytest tests/
  ```

### 3. CI/CD Pipeline
- On push/PR to `main`, GitHub Actions will:
  - Run unit tests and schema checks
  - Run ETL and ML scripts
  - Upload model artifacts


### 4. Querying Iceberg Table
- Use Spark SQL or PySpark:
  ```python
  spark.read.format("iceberg").load("local_catalog.corp_db.corporate_registry").show()
  ```

### 5. Viewing Registered Model
- Start MLflow UI:
  ```bash
  mlflow ui --backend-store-uri ./mlruns
  ```
- Open http://localhost:5000 to view model runs and artifacts.


## Entity Resolution Heuristic
- Cleans and normalizes corporate names and addresses from both sources.
- Generates a unique `corp_id` by combining cleaned name and normalized address.
- Merges records from both sources on this `corp_id` to create a unified, deduplicated view.

## Iceberg Merge/Upsert Strategy
- Uses Spark SQL `MERGE INTO` for transactional upserts into the `corporate_registry` table.

## Cloud Deployment
- For AWS/GCP/Azure, update the Spark and Iceberg configs in the scripts to use the appropriate metastore and storage.
- (Bonus) Use the provided Airflow DAG and Terraform template for orchestration and IaC.


## Sample Data
- See `sample_data/source1_supply_chain.csv` and `sample_data/source2_financial.csv` for example corporate data sources.

---


## Bonus: Orchestration & IaC
- See `infra/airflow_dag.py` and `infra/main.tf` for Airflow and Terraform examples.
