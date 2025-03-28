Airflow DAG: History Load

Overview

This project is an Airflow DAG designed to perform historical data loading and validation in a GCP BigQuery environment. The pipeline follows a structured execution process with multiple SQL-based data load tasks and validation checks.

Project Structure

dags/
├── landing_to_core_load/
│   ├── config_files/          # Contains YAML config files for table creation
│   ├── fcty_code/             # Python scripts for DAG execution
│   ├── packages/              # Utility Python files (web_hook.py, single_table_validation.py)
│   ├── data_validation/       # YAML files for data validation (e.g., jsontoyaml.yaml)
│   ├── history_load/          # SQL files for historical data load
│   ├── incremental_load/      # SQL files for incremental data load

DAG Configuration (history_load.yaml)

The DAG is configured using a YAML file (history_load.yaml), which defines execution schedules, tasks, dependencies, and validation logic.

Key Configuration Parameters

DAG Metadata

dagname: history_load

dagname: Name of the DAG.

dag_schedule: "0 12 * * *"

dag_schedule: Specifies the execution schedule using cron format (runs daily at 12 PM UTC).

retry: 3
maxrun: 1
depends_on_past: false
catchup: false
start_date: 'datetime.datetime(2025, 3, 26, 0, 0)'

retry: Number of retries for failed tasks.

maxrun: Limits the number of concurrent DAG runs.

depends_on_past: Specifies whether each task depends on previous DAG runs.

catchup: Determines if past scheduled runs should be executed.

start_date: Defines when the DAG should start running.

Task Dependencies

processing_dependencies:
  - start >> intial_load
  - intial_load >> merge_asset_flow >> postprocess_audit
  - intial_load >> merge_bet >> postprocess_audit
  - intial_load >> merge_bet_leg >> postprocess_audit
  - postprocess_audit >> validate_1 >> table_1_duplicate_check >> table_1_null_check >> send_email
  - validate_1 >> table_2_duplicate_check >> table_2_null_check >> send_email
  - send_email >> end

Defines the sequential execution of tasks, ensuring proper data processing flow.

Environment Configuration

env: bespin-us-demo
webhook_url: "https://chat.googleapis.com/v1/spaces/..."

env: Specifies the GCP project where execution takes place.

webhook_url: URL used for sending alerts via Google Chat webhook.

Data Load Tasks

Each task executes an SQL file with parameters replaced dynamically.

data_load_tasks:
  intial_load:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/initial_load.sql"
    replace_parameters:
      project: "bespin-us-demo"

queryfilepath: Specifies the location of the SQL file.

replace_parameters: Defines runtime parameters (e.g., GCP project).

Similar configurations exist for:

merge_asset_flow

merge_bet

merge_bet_leg

postprocess_audit

Data Validation (jsontoyaml.yaml)

The jsontoyaml.yaml file defines validation rules for the loaded data.

data_validation:
  validate_1:
    json_path: "/home/airflow/gcs/dags/landing_to_core_load/data_validation/jsontoyaml.yaml"

json_path: Points to the validation configuration file.

Single Table Validation

single_table_validation:
  table_1:
    table_details:
      project: "bespin-us-demo"
      dataset: "pso_data_validator"
      table_name: "results"

Defines validation rules for table_1 and table_2, including:

duplicate_check: Ensures unique primary keys.

null_check: Ensures required columns are not null.

Supporting Python Scripts

web_hook.py

Sends success/failure alerts to a webhook.

single_table_validation.py

Inserts validation results into BigQuery.

Performs duplicate and null checks.

Execution Steps

Setup Airflow Environment

export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
airflow scheduler & airflow webserver

Deploy DAG & Configurations

Upload history_load.yaml and jsontoyaml.yaml to the DAGs folder.

Trigger DAG Execution

Manually trigger via UI or schedule execution.

Monitor Execution

Check logs and validation results in BigQuery.

Receive Alerts


    L --> M[end]

Google Chat webhook sends alerts based on DAG success or failure.

Conclusion

This Airflow DAG automates historical data loading with validation checks, ensuring data integrity and compliance within the GCP environment. 🚀
