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

# --- IMPORTANT: Replace with YOUR unique S3 bucket name ---
S3_BUCKET_NAME = "b-surampudi-weather-pipeline-raw-12345" 
# --- IMPORTANT: Replace if you changed the AWS connection ID ---
AWS_CONN_ID = "aws_default" 
# --- IMPORTANT: Replace if you changed the Postgres connection ID ---
DBT_CONN_ID = "dbt_postgres_conn" 

# Paths inside the Airflow container (based on docker-compose volume mounts)
PYTHON_SCRIPTS_PATH = "/opt/airflow/scripts"
DBT_PROJECT_PATH = "/opt/airflow/dbt_project"
# Temporary local path within the container for the extracted file
LOCAL_EXTRACT_PATH_TEMPLATE = "/tmp/weather_extract_{{ ds_nodash }}.json" 

# dbt project configuration for Cosmos
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="weather_project", # Match profile name in dbt_project.yml
    target_name="dev",             # Match target name in profiles.yml
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=DBT_CONN_ID,
        # Schema where dbt will build models (override if needed)
        profile_args={"schema": "public"}, 
    )
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# --- DAG Definition ---
@dag(
    dag_id="weather_data_pipeline",
    start_date=datetime.datetime(2025, 10, 27), # Set to today or intended start
    schedule="@daily",                         # Run daily
    catchup=False,                             # Don't run for past missed schedules
    tags=["weather", "pipeline", "dbt", "s3"],
    doc_md="""
    ### Weather Data Pipeline DAG
    Orchestrates the extraction of weather data from an API, loading to S3,
    simulated load to warehouse, and transformation using dbt.
    """
)
def weather_data_pipeline():

    start = EmptyOperator(task_id="start")

    @task(task_id="extract_weather_data")
    def extract_data(**context):
        """
        Runs the Python script to fetch weather data from API.
        Returns the path to the saved JSON file.
        """
        import sys
        import subprocess

        # Get the logical date for this DAG run
        logical_date_str = context["ds_nodash"]
        output_filepath = f"/tmp/weather_data_{logical_date_str}.json" # Unique path per run

        script_path = os.path.join(PYTHON_SCRIPTS_PATH, "extract_weather_data.py")

        print(f"Running extraction script: {script_path}")
        print(f"Expected output file: {output_filepath}") # We won't use this directly, script saves its own

        # Execute the script using python executable within the container env
        # This ensures it uses the installed libraries (like requests)
        process = subprocess.run(
            [sys.executable, script_path], 
            capture_output=True, 
            text=True,
            check=False # Don't raise error immediately, check returncode later
        )

        print("Script STDOUT:")
        print(process.stdout)
        print("Script STDERR:")
        print(process.stderr)

        if process.returncode != 0:
            raise Exception(f"Python script failed with return code {process.returncode}")

        # --- Find the actual saved files --- 
        # The script saves files like /tmp/weather_data/weather_CITY_DATE.json
        # We need to find these files to upload them.
        # NOTE: This assumes the script always saves to the same sub-directory '/tmp/weather_data'
        script_output_dir = "/tmp/weather_data"
        saved_files = []
        if os.path.exists(script_output_dir):
             # Find files matching the pattern for today's run
            pattern = f"weather_*_{logical_date_str}.json"
            for filename in os.listdir(script_output_dir):
                if filename.endswith(f"_{logical_date_str}.json"):
                     saved_files.append(os.path.join(script_output_dir, filename))

        if not saved_files:
            raise FileNotFoundError("Extraction script ran but no output JSON files found in /tmp/weather_data for today's date.")

        print(f"Found saved files: {saved_files}")
        return saved_files # Return list of file paths

    @task(task_id="upload_to_s3")
    def upload_data(local_filepaths: list):
        """
        Uploads the extracted JSON files to S3.
        """
        if not local_filepaths:
            raise ValueError("No local file paths provided for upload.")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_keys = []

        for local_filepath in local_filepaths:
            if not os.path.exists(local_filepath):
                print(f"Warning: File not found {local_filepath}, skipping upload.")
                continue

            # Define S3 key (path within the bucket) including date partitioning
            file_basename = os.path.basename(local_filepath)
            # Assumes filename format like weather_CITY_YYYYMMDD.json
            date_str = file_basename.split('_')[-1].replace('.json','') 
            year = date_str[0:4]
            month = date_str[4:6]
            day = date_str[6:8]

            s3_key = f"raw/weather/{year}/{month}/{day}/{file_basename}"

            print(f"Uploading {local_filepath} to S3 bucket {S3_BUCKET_NAME} at key {s3_key}")

            s3_hook.load_file(
                filename=local_filepath,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True # Overwrite if file exists
            )
            uploaded_keys.append(s3_key)
            # Optional: Clean up local file after upload
            # os.remove(local_filepath) 

        if not uploaded_keys:
             raise Exception("Upload task ran but failed to upload any files to S3.")

        print(f"Successfully uploaded files to S3: {uploaded_keys}")
        return uploaded_keys # Return list of S3 keys

    # Placeholder Task - Replace with actual loading logic later
    simulate_load_to_warehouse = EmptyOperator(
        task_id="simulate_load_to_warehouse",
        doc_md="""
        ### Simulate Load to Warehouse
        **Placeholder:** In a real pipeline, this task would load data 
        from S3 (using the keys from the previous task) into the 
        raw Postgres table (e.g., using S3ToPostgresOperator or custom script).
        For now, ensure data is manually loaded into `public.raw_weather` 
        in the `airflow` Postgres DB before running dbt.
        """
    )

    # dbt Task Group (using Cosmos)
    run_dbt_project = DbtTaskGroup(
        group_id="run_dbt_project",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # Add arguments needed by dbt commands if necessary
        # operator_args={"install_deps": True}, 
    )

    end = EmptyOperator(task_id="end")

    # --- Task Dependencies ---
    extracted_files = extract_data()
    s3_keys = upload_data(extracted_files)

    start >> extracted_files >> s3_keys >> simulate_load_to_warehouse >> run_dbt_project >> end

# Instantiate the DAG
weather_data_pipeline()