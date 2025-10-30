Welcome\! This tutorial will go deep into **all** the concepts from `weather_data_pipeline.py` file. We'll use this code as the primary example to explore everything from the core philosophy to advanced, production-grade patterns like dbt integration.

### 🚀 The Core Philosophy: Why Airflow?

At its heart, Airflow is an orchestrator. Its job isn't to *do* the data processing but to **manage, schedule, and monitor** the workflows that do.

Think of Airflow as the **head chef of a restaurant kitchen**:

  * **The Recipe:** `weather_data_pipeline.py` file.
  * **The Cooking Steps:** Our `@task` functions (e.g., `extract_data_task`, `upload_to_s3`).
  * **The Chef (Airflow):** Doesn't chop or grill, but ensures the line cooks (`Workers`) do their tasks in the right order (dependencies), at the right time (`schedule`), and retries if they mess up.

-----

## 🔬 Part 1: Anatomy of Our `weather_data_pipeline`

Our DAG uses a modern and powerful structure. It combines the **TaskFlow API** (`@dag`, `@task`) for Python tasks, a **`subprocess`** call to run external scripts, and the **Astronomer Cosmos** library for seamless dbt integration.

Let's walk through it, task by task.

### 1.1. The `@dag` Decorator: The "Recipe Card"

This is the main container for Our entire workflow.

```python
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
    ...
```

  * `dag_id`: The unique name for Our DAG in the Airflow UI.
  * `start_date`: You've correctly set this as a **static, fixed datetime**. This is the most critical parameter for the scheduler.
  * `schedule="@daily"`: This tells the scheduler to create a DAG Run for each day, *after* that day has finished (e.g., the run for Oct 27th will be triggered just after midnight on Oct 28th).
  * `catchup=False`: **Essential.** You've correctly set this to `False`. If it were `True`, Airflow would try to run this DAG for every single day between Our `start_date` and today.
  * `doc_md`: This is a best practice. You're using Airflow's built-in documentation feature to explain what the DAG does and (critically) what its manual steps are.

-----

### 1.2. The Tasks: "Cooking Steps" in Our ELT Pipeline

Our pipeline follows a clear **ELT (Extract, Load, Transform)** pattern.

#### `start` (EmptyOperator)

```python
    start = EmptyOperator(task_id="start")
```

This is a simple `EmptyOperator`. It does nothing except act as a clear, visual starting point for Our pipeline in the graph view.

#### `extract_data_task` (The "E" in ELT)

```python
    @task(task_id="extract_weather_data")
    def extract_data_task(**context):
        # ...
        logical_date_str = context["ds_nodash"]
        script_path = os.path.join(PYTHON_SCRIPTS_PATH, "extract_weather_data.py")
        ti = context["ti"] # TaskInstance object

        print(f"Running extraction script: {script_path} for date {logical_date_str}")

        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True, text=True, check=False
        )
        # ... (error checking) ...
        
        # ... (logic to find saved files) ...
        if not saved_files:
            raise FileNotFoundError(f"No output JSON files found in {LOCAL_WEATHER_DATA_DIR} for date {logical_date_str}.")

        print(f"Found saved files: {saved_files}")
        # Push file paths to XCom for the next task
        ti.xcom_push(key='extracted_file_paths', value=saved_files)
```

This is Our first major task. Instead of putting all the Python logic inside the DAG file, you're using this task as a **runner** for an external script (`extract_weather_data.py`).

  * **`subprocess.run`**: This command runs Our Python script in a separate process. This is a common pattern for reusing existing scripts or isolating dependencies.
  * **`context["ds_nodash"]`**: You're using the Airflow context to get the logical execution date (e.g., `20251027`). This is a **best practice for idempotence**, ensuring that if this task re-runs, it looks for files from the *same* day.
  * **`ti.xcom_push(...)`**: This is a **manual XCom push**. After the script runs, this task looks in the `/tmp/weather_data` directory, finds the files that were just created, and "pushes" a list of their filepaths to Airflow's metadata database (XCom). This list is now available for the next task.

#### `upload_data_task` (The first "L" in ELT)

```python
    @task(task_id="upload_to_s3")
    def upload_data_task(**context):
        """
        Pulls file paths from XCom and uploads the extracted JSON files to S3.
        Pushes list of uploaded S3 keys to XCom.
        """
        ti = context["ti"] # TaskInstance object
        local_filepaths = ti.xcom_pull(task_ids='extract_weather_data', key='extracted_file_paths')
        
        # ... (error checking) ...

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        uploaded_s3_keys = []

        for local_filepath in local_filepaths:
            # ... (logic to build s3_key) ...
            s3_key = f"raw/weather/{year}/{month}/{day}/{file_basename}"
            
            print(f"Uploading {local_filepath} to S3 bucket {S3_BUCKET_NAME} at key {s3_key}")
            s3_hook.load_file(filename=local_filepath, key=s3_key, bucket_name=S3_BUCKET_NAME, replace=True)
            uploaded_s3_keys.append(f"s3://{S3_BUCKET_NAME}/{s3_key}") # Store full S3 path
        
        # ... (error checking) ...

        # Push S3 keys to XCom if needed by subsequent tasks (e.g., loading)
        ti.xcom_push(key='uploaded_s3_keys', value=uploaded_s3_keys)
```

This task is the other half of Our extraction.

  * **`ti.xcom_pull(...)`**: This is a **manual XCom pull**. It's the "read" step, retrieving the list of filepaths that the `extract_data_task` pushed.
  * **`S3Hook(aws_conn_id=AWS_CONN_ID)`**: This is the standard Airflow tool for talking to AWS. It uses the connection you've stored in the Airflow UI (named `aws_default`) to securely get the credentials it needs.
  * **S3 Key Partitioning**: You're dynamically building the S3 key: `s3_key = f"raw/weather/{year}/{month}/{day}/{file_basename}"`. This is a **data engineering best practice**. By partitioning Our raw data by date, you make it *much* faster and cheaper to query later.

#### `simulate_load_to_warehouse` (The "Missing Link")

```python
    simulate_load_to_warehouse = EmptyOperator(
        task_id="simulate_load_to_warehouse",
        doc_md="""
        ### Simulate Load to Warehouse (Manual Step Required!)
        **Placeholder:** Manually load the JSON data ...
        into the `public.raw_weather` table ...
        """
    )
```

This `EmptyOperator` is a **critical placeholder**. It does no work, but it holds a very important spot in Our pipeline and clearly documents the manual step required.

  * This represents the second "L" in Our **ELT** pipeline: **Load (to S3)**, then **Load (to Warehouse)**. The `doc_md` is the real instruction for the user.

#### `run_dbt_project` (The "T" in ELT)

```python
    # dbt Task Group (using Cosmos)
    run_dbt_project = DbtTaskGroup(
        group_id="run_dbt_project",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        # Default behavior runs `dbt run` then `dbt test`
    )
```

This is the most powerful part of Our pipeline. You're using **Astronomer Cosmos** (`DbtTaskGroup`) to integrate Our dbt project directly into Airflow.

  * **`DbtTaskGroup`**: This isn't a single task. It's a special operator that reads Our `dbt_project` directory, finds all Our models (like `staging/` and `analytics/`), and automatically creates a visual Task Group in the Airflow UI that matches Our dbt model dependencies.
  * **`DBT_PROJECT_CONFIG`**: This tells Cosmos *where* Our dbt project lives (`/opt/airflow/dbt_project`).
  * **`DBT_PROFILE_CONFIG`**: This is the glue. It tells Cosmos *how* to connect to Our database by mapping dbt's `profile_name` (`weather_project`) to an Airflow Connection ID (`DBT_CONN_ID`). This is a fantastic way to manage credentials.

#### `end` (EmptyOperator)

```python
    end = EmptyOperator(task_id="end")
```

Just like `start`, this is a dummy task that provides a clean visual end to Our pipeline.

### 1.3. The Task Dependencies

```python
    # --- Task Dependencies ---
    # Define the order of execution using TaskFlow API results
    extract_task_instance = extract_data_task()
    upload_task_instance = upload_data_task() # XCom is pulled inside the task

    start >> extract_task_instance >> upload_task_instance >> simulate_load_to_warehouse >> run_dbt_project >> end
```

Our dependency chain is clear and linear. Each task waits for the previous one to succeed, creating a reliable, step-by-step ELT workflow.

-----

## 🛠️ Part 2: Deep Dive on Key Concepts in Our DAG

Our DAG showcases several important patterns. Let's explore them.

### `subprocess` vs. In-Task Python

You used `subprocess.run` to call an external script.

  * **Pros (Why you did this):**
      * **Isolation:** If `extract_weather_data.py` has a complex dependency (like `requests` or `pandas`), running it in a subprocess prevents it from conflicting with Airflow's own libraries.
      * **Reusability:** You can run that script from Our laptop *or* from Airflow without changing it.
  * **Cons (What to watch for):**
      * **Complexity:** It's harder to manage. You have to manually find the files, pass data via XCom, and log the `stdout`/`stderr`.
      * **The "Airflow-Native" Way:** The alternative is to move the logic from `extract_weather_data.py` *directly* into the `extract_data_task` function. This would make the code simpler to read in one place.

### Manual vs. Automatic XComs

You used `ti.xcom_push` and `ti.xcom_pull`. This is the **manual** way.

  * **Why you *had* to do this:** Because Our `extract_data_task` function's job was just to *run* the `subprocess`, it couldn't `return` the filepaths directly. The `subprocess` wrote files, so Our task had to go find them *after* the process finished and then *manually push* the list.
  * **The "Automatic" Way:** The modern TaskFlow API (`@task`) lets you just `return` a value.
      * If you refactored `extract_data_task` to do the work "natively" (see above), you could just do this:
    <!-- end list -->
    ```python
    @task
    def extract_data_task():
        # ... python logic to fetch data ...
        # ... logic to save files ...
        return saved_files # <-- Automatic XCom push

    @task
    def upload_data_task(extracted_file_paths: list): # <-- Automatic XCom pull
        # 'extracted_file_paths' is now a Python list
        for local_filepath in extracted_file_paths:
            ...
    ```

### `S3Hook` and AWS Connections

Our `S3Hook(aws_conn_id=AWS_CONN_ID)` is the perfect example of how Airflow handles credentials.

  * You've set `AWS_CONN_ID = "aws_default"`.
  * In a **production AWS environment (like MWAA)**, you would *not* put an Access Key and Secret Key into the `aws_default` connection.
  * Instead, you would grant the **Airflow Execution Role** (the IAM Role for the whole Airflow environment) permissions to access S3 (e.g., `AmazonS3FullAccess`).
  * You'd leave the `aws_default` connection in the UI **completely blank**.
  * The `S3Hook` is smart: if it finds a blank connection, it automatically uses the temporary IAM credentials from its environment. This is the most secure and professional way to manage AWS access.

-----

## 📈 Part 3: How to Evolve This Pipeline (Next Steps)

Our DAG is a fantastic and complete ELT pipeline. The most obvious next step is to **automate the "Simulated" part**.

### Automating the "L": Replacing the `EmptyOperator`

Our `simulate_load_to_warehouse` task is a manual placeholder. You can replace it with a real task to create a fully automated pipeline.

**Option 1: Use a Pre-built Operator**
You could use the `S3ToPostgresOperator` (since Our dbt profile is Postgres).

```python
from airflow.providers.amazon.aws.transfers.s3_to_postgres import S3ToPostgresOperator

# This would replace Our EmptyOperator
load_to_warehouse = S3ToPostgresOperator(
    task_id="load_s3_to_postgres",
    postgres_conn_id=DBT_CONN_ID, # Our Postgres conn
    s3_bucket=S3_BUCKET_NAME,
    # Use templating to get the right folder for this run
    s3_key="raw/weather/{{ ds_nodash[0:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/",
    schema="public",
    table="raw_weather",
    copy_options=["FORMAT AS JSON 'auto'"], # Tell Postgres how to parse the JSON
)
```

**Option 2: Use a Custom Python Task (Gives more control)**
This is often preferred. You'd write a Python function that uses the `PostgresHook` to run the `COPY` command.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task(task_id="load_s3_to_warehouse")
def load_s3_to_warehouse(**context):
    pg_hook = PostgresHook(postgres_conn_id=DBT_CONN_ID)
    s3_keys = context["ti"].xcom_pull(task_ids='upload_to_s3', key='uploaded_s3_keys')
    
    if not s3_keys:
        raise ValueError("No S3 keys were passed from the upload task.")

    # In a real AWS setup, Redshift/Postgres needs credentials
    # to read from S3. This is often an IAM Role ARN.
    # You would store this ARN in an Airflow Variable.
    # iam_role_arn = Variable.get("postgres_s3_access_role_arn")
    # credentials_str = f"aws_iam_role={iam_role_arn}"

    # For now, assuming local Postgres or simple auth
    # For a real Redshift/Postgres S3 load, you'd add the CREDENTIALS string.

    # We need to clear the table first for idempotence
    pg_hook.run("TRUNCATE TABLE public.raw_weather;") # Example for a full refresh

    for s3_path in s3_keys:
        # Note: The COPY command for S3 to Postgres is complex
        # and requires the aws_s3 extension or other methods.
        # A more common pattern is S3 to Redshift, which is simpler.
        # For Postgres, you might download the file then COPY FROM STDIN.
        
        # Example for Redshift (which Our original project used):
        # copy_query = f"""
        #     COPY public.raw_weather
        #     FROM '{s3_path}'
        #     CREDENTIALS '{credentials_str}'
        #     FORMAT AS JSON 'auto';
        # """
        # pg_hook.run(copy_query)
        
        print(f"Simulating COPY command for {s3_path}")
        pass # Replace with Our actual COPY logic

# ... and in Our dependency chain ...
upload_task_instance >> load_s3_to_warehouse() >> run_dbt_project
```

By making this one change, you would have a 100% automated, production-grade ELT pipeline.
