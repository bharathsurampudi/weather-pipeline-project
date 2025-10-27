# Simple Batch Data Pipeline (API to S3 to dbt via Airflow)

## Project Goal

This project implements a foundational batch data pipeline using a modern ELT (Extract, Load, Transform) approach. It demonstrates core data engineering principles by orchestrating the flow of data from a public API into cloud storage (AWS S3) and transforming it within a local data warehouse (PostgreSQL) using dbt, all managed by Apache Airflow.

The primary objective is to showcase practical skills in building, configuring, and running a data pipeline using industry-standard tools, suitable for a data engineering portfolio.

## Tech Stack & Component Roles

This pipeline utilizes several key technologies, each playing a specific role:

* **Apache Airflow (Orchestrator):**
    * **Role:** Acts as the central brain of the pipeline. It defines the workflow (DAG), schedules runs (e.g., daily), triggers tasks in the correct order, handles dependencies between tasks, monitors execution, logs output, and manages retries on failure.
    * **Setup:** Runs via Docker Compose using the official Airflow image. Requires configuration of connections (AWS, Postgres) via the UI and installation of necessary Python packages (`astronomer-cosmos`, `boto3`, etc.) defined in `docker-compose.yaml`.
* **dbt (Data Build Tool - Transformation):**
    * **Role:** Handles the "T" in ELT. Transforms raw data loaded into Postgres into clean, reliable, analytics-ready tables using SQL `SELECT` statements defined in `.sql` models. Manages dependencies between models and runs data quality tests.
    * **Setup:** The dbt project files reside in `dbt_project/`. It requires a `profiles.yml` (typically outside the project, e.g., in `~/.dbt/`) for local runs, defining connection details to the Postgres database. Airflow uses `astronomer-cosmos` to parse this project and run models/tests.
* **Astronomer Cosmos (Airflow-dbt Integration):**
    * **Role:** An Airflow provider that dynamically generates Airflow tasks or task groups directly from a dbt project. It allows Airflow to run dbt models and tests as native Airflow tasks, leveraging Airflow connections for database credentials.
    * **Setup:** Installed as a Python package within the Airflow Docker environment (`_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`). Configured within the Airflow DAG (`pipeline_dag.py`) via `ProjectConfig` and `ProfileConfig`.
* **AWS S3 (Data Lake / Cloud Storage):**
    * **Role:** Serves as the landing zone (data lake) for raw, unprocessed data extracted from the API. Provides scalable, durable, and cost-effective storage.
    * **Setup:** The S3 bucket is provisioned using Terraform. Airflow requires an AWS connection (`aws_default`) with appropriate credentials (Access Key ID and Secret Access Key) to interact with the bucket (e.g., upload files).
* **PostgreSQL (Data Warehouse):**
    * **Role:** Acts as the data warehouse where transformations occur. Stores both the raw data (loaded from S3/local file in this simulation) and the final transformed tables created by dbt.
    * **Setup:** Runs as a service within Docker Compose (either alongside Airflow or separately). Airflow and dbt require connection details (host, port, user, password, database name) configured via Airflow connections and dbt profiles.
* **Terraform (Infrastructure as Code):**
    * **Role:** Manages the cloud infrastructure (the S3 bucket) declaratively. Ensures the required AWS resources are created consistently and can be easily destroyed.
    * **Setup:** Terraform code (`.tf` files) resides in the `terraform/` directory. Requires AWS CLI configured with credentials (`aws configure`) to interact with your AWS account. The bucket name must be globally unique.
* **Python (Extraction & Glue Code):**
    * **Role:** Used for scripting tasks within Airflow. Specifically, fetching data from the API (`requests` library) and uploading the data file to S3 (`boto3` library).
    * **Setup:** Scripts are placed in `python_scripts/`. Required libraries (`requests`, `boto3`, `pandas`) must be installed in the Airflow Docker environment.
* **Docker & Docker Compose (Containerization):**
    * **Role:** Provides isolated, reproducible environments for running services like Airflow and Postgres. Simplifies setup and dependency management, ensuring the pipeline runs consistently across different machines.
    * **Setup:** Configuration is defined in `docker-compose.yaml` files within the `airflow/` directory (and potentially a separate one for Postgres if not combined). Defines services, networks, volumes, and environment variables.

## Data Flow Explained (ELT)

1.  **Extract (Airflow Python Task):** Runs `python_scripts/extract_weather_data.py` to fetch JSON data from OpenWeatherMap API, saves files locally in the container (e.g., `/tmp/weather_data/weather_CITY_YYYYMMDD.json`). Passes file paths via XCom.
2.  **Load (Airflow Python Task):** Receives file paths via XCom. Uses `boto3` and `aws_default` connection to upload JSON files to the S3 bucket under `raw/weather/YYYY/MM/DD/`. Passes S3 keys via XCom.
3.  **Simulated Load to Warehouse (Airflow EmptyOperator - MANUAL STEP):** Placeholder task. **Requires manual intervention:** After this task succeeds, user must connect to the Postgres DB (`localhost:5432`) and `INSERT` the JSON data from S3/local files for the current run date into the `public.raw_weather` table.
4.  **Transform (Airflow DbtTaskGroup via Cosmos):** Uses `dbt_postgres_conn` connection. Runs `dbt run` then `dbt test` against the `dbt_project/`, transforming data in Postgres based on models in `dbt_project/models/` (creating `staging.stg_weather_data` and `analytics.fct_weather_daily`).
5.  **Orchestration (Airflow):** The `weather_data_pipeline` DAG ensures tasks run in order: `start >> extract >> upload >> simulate_load >> run_dbt >> end`. Runs daily.
6.  **Infrastructure Management (Terraform):** `terraform apply` creates the S3 bucket before the pipeline runs; `terraform destroy` removes it.
