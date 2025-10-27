import datetime
import json
import os
from pathlib import Path

# Airflow Imports
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Hook to interact with S3

# Cosmos (dbt Integration) Imports
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping

# --- Constants & Configuration ---

# --- IMPORTANT: Ensure this matches YOUR unique S3 bucket name ---
S3_BUCKET_NAME = "b-surampudi-weather-pipeline-raw-12345" # <<< VERIFY THIS NAME
AWS_CONN_ID = "aws_default"
DBT_CONN_ID = "dbt_postgres_conn"

# Paths inside the Airflow container
PYTHON_SCRIPTS_PATH = "/opt/airflow/scripts"
DBT_PROJECT_PATH = "/opt/airflow/dbt_project"
LOCAL_WEATHER_DATA_DIR = "/tmp/weather_data" # Defined in extract script

# dbt project configuration for Cosmos
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="weather_project", # Match profile name in dbt_project.yml
    target_name="dev",             # Match target name in profiles.yml
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=DBT_CONN_ID,
        # dbt schemas are defined in dbt_project.yml now,
        # override specific schema for profile if needed, otherwise use dbt defaults
        profile_args={"schema": "public"}, # Or remove if dbt_project.yml defines staging/analytics schemas
    )
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# --- DAG Definition ---
@dag(
    dag_id="weather_data_pipeline",
    start_date=datetime.datetime(2025, 10, 27), # Use current/recent date
    schedule="@daily",
    catchup=False,
    tags=["weather", "pipeline", "dbt", "s3"],
    doc_md="""
    ### Weather Data Pipeline DAG
    Orchestrates the extraction of weather data from an API, loading to S3,
    simulated load to warehouse, and transformation using dbt.
    **Note:** Loading from S3 to the warehouse (Postgres) is currently a placeholder.
    Data must be manually loaded into `public.raw_weather` before dbt tasks run.
    """
)
def weather_data_pipeline():

    start = EmptyOperator(task_id="start")

    @task(task_id="extract_weather_data")
    def extract_data_task(**context):
        """
        Runs the Python script to fetch weather data from API.
        Pushes list of saved file paths to XCom.
        """
        import sys
        import subprocess

        logical_date_str = context["ds_nodash"]
        script_path = os.path.join(PYTHON_SCRIPTS_PATH, "extract_weather_data.py")
        ti = context["ti"] # TaskInstance object

        print(f"Running extraction script: {script_path} for date {logical_date_str}")

        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True, text=True, check=False
        )

        print("Script STDOUT:"); print(process.stdout)
        print("Script STDERR:"); print(process.stderr)

        if process.returncode != 0:
            raise Exception(f"Python script failed with return code {process.returncode}")

        # --- Find the actual saved files ---
        saved_files = []
        if os.path.exists(LOCAL_WEATHER_DATA_DIR):
            for filename in os.listdir(LOCAL_WEATHER_DATA_DIR):
                # Ensure we only grab files matching the current run's date
                if filename.endswith(f"_{logical_date_str}.json"):
                     saved_files.append(os.path.join(LOCAL_WEATHER_DATA_DIR, filename))

        if not saved_files:
            raise FileNotFoundError(f"No output JSON files found in {LOCAL_WEATHER_DATA_DIR} for date {logical_date_str}.")

        print(f"Found saved files: {saved_files}")
        # Push file paths to XCom for the next task
        ti.xcom_push(key='extracted_file_paths', value=saved_files)

    @task(task_id="upload_to_s3")
    def upload_data_task(**context):
        """
        Pulls file paths from XCom and uploads the extracted JSON files to S3.
        Pushes list of uploaded S3 keys to XCom.
        """
        ti = context["ti"] # TaskInstance object
        local_filepaths = ti.xcom_pull(task_ids='extract_weather_data', key='extracted_file_paths')

        if not local_filepaths:
            raise ValueError("No local file paths received from extract_weather_data task via XCom.")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_s3_keys = []

        for local_filepath in local_filepaths:
            if not os.path.exists(local_filepath):
                print(f"Warning: File not found {local_filepath}, skipping upload.")
                continue

            file_basename = os.path.basename(local_filepath)
            # Assumes filename format like weather_CITY_YYYYMMDD.json
            try:
                date_str = file_basename.split('_')[-1].replace('.json','')
                year, month, day = date_str[0:4], date_str[4:6], date_str[6:8]
                s3_key = f"raw/weather/{year}/{month}/{day}/{file_basename}"
            except IndexError:
                print(f"Warning: Could not parse date from filename {file_basename}, using default path.")
                s3_key = f"raw/weather/unknown_date/{file_basename}" # Fallback path

            print(f"Uploading {local_filepath} to S3 bucket {S3_BUCKET_NAME} at key {s3_key}")
            s3_hook.load_file(filename=local_filepath, key=s3_key, bucket_name=S3_BUCKET_NAME, replace=True)
            uploaded_s3_keys.append(f"s3://{S3_BUCKET_NAME}/{s3_key}") # Store full S3 path
            # os.remove(local_filepath) # Optional cleanup

        if not uploaded_s3_keys:
             raise Exception("Upload task ran but failed to upload any files to S3.")

        print(f"Successfully uploaded files to S3: {uploaded_s3_keys}")
        # Push S3 keys to XCom if needed by subsequent tasks (e.g., loading)
        ti.xcom_push(key='uploaded_s3_keys', value=uploaded_s3_keys)

    # --- SIMULATED LOAD STEP ---
    simulate_load_to_warehouse = EmptyOperator(
        task_id="simulate_load_to_warehouse",
        doc_md="""
        ### Simulate Load to Warehouse (Manual Step Required!)
        **Placeholder:** Manually load the JSON data (corresponding to this run's date)
        from S3 (or the local `/tmp/weather_data` files inside worker container) into the `public.raw_weather` table
        in the `airflow` Postgres DB **before** the dbt tasks can succeed.
        Use a SQL client connected to `localhost:5432`.
        See dbt local testing step for example INSERT.
        """
    )

    # dbt Task Group (using Cosmos)
    run_dbt_project = DbtTaskGroup(
        group_id="run_dbt_project",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # Default behavior runs `dbt run` then `dbt test`
    )

    end = EmptyOperator(task_id="end")

    # --- Task Dependencies ---
    # Define the order of execution using TaskFlow API results
    extract_task_instance = extract_data_task()
    upload_task_instance = upload_data_task() # XCom is pulled inside the task

    start >> extract_task_instance >> upload_task_instance >> simulate_load_to_warehouse >> run_dbt_project >> end

# Instantiate the DAG
weather_data_pipeline()