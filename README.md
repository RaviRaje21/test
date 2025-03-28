# Airflow DAG: History Load

## Overview

This project is an Airflow DAG designed to perform historical data loading and validation in a GCP BigQuery environment. The pipeline follows a structured execution process with multiple SQL-based data load tasks and validation checks.

## Project Structure

```
dags/
├── landing_to_core_load/
│   ├── config_files/          # Contains dag config files
│   ├── fcty_code/             # Python scripts for DAG execution
│   ├── packages/              # Utility Python files (web_hook.py, single_table_validation.py)
│   ├── data_validation/       # YAML files for data validation (e.g., validation1.yaml)
│   ├── history_load/          # SQL files for historical data load
│   ├── incremental_load/      # SQL files for incremental data load
```

## DAG Configuration (history_load.yaml / incremental_load.yaml)

The DAG is configured using a YAML files, which defines execution schedules, tasks, dependencies, and validation logic.

### Key Configuration Parameters

#### DAG Metadata

```yaml
dagname: history_load
```

- **dagname**: Name of the DAG.

```yaml
dag_schedule: "0 12 * * *"
```

- **dag_schedule**: Specifies the execution schedule using cron format (runs daily at 12 PM UTC).

```yaml
retry: 3
maxrun: 1
depends_on_past: false
catchup: false
start_date: 'datetime.datetime(2025, 3, 26, 0, 0)'
```

- **retry**: Number of retries for failed tasks.
- **maxrun**: Limits the number of concurrent DAG runs.
- **depends_on_past**: Specifies whether each task depends on previous DAG runs.
- **catchup**: Determines if past scheduled runs should be executed.
- **start_date**: Defines when the DAG should start running.

#### Task Dependencies

```yaml
processing_dependencies:
  - start >> intial_load
  - intial_load >> merge_asset_flow >> postprocess_audit
  - intial_load >> merge_bet >> postprocess_audit
  - intial_load >> merge_bet_leg >> postprocess_audit
  - postprocess_audit >> validate_1 >> table_1_duplicate_check >> table_1_null_check >> send_email
  - validate_1 >> table_2_duplicate_check >> table_2_null_check >> send_email
  - send_email >> end
```

Defines the sequential execution of tasks, ensuring proper data processing flow.

#### Environment Configuration

```yaml
env: <PROJECT_NAME>
webhook_url: "https://chat.googleapis.com/v1/spaces/..."
```

- **env**: Specifies the GCP project where execution takes place.
- **webhook_url**: URL used for sending alerts via Google Chat webhook.

### Data Load Tasks

Each task executes an SQL file with parameters replaced dynamically.

```yaml
data_load_tasks:
  intial_load:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/initial_load.sql"
    replace_parameters:
      project: "<PROJECT_NAME>"
  
  merge_asset_flow:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/merge_bet_asset_flow.sql"
    replace_parameters:
      project: "<PROJECT_NAME>"
  
  merge_bet:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/merge_bet.sql"
    replace_parameters:
      project: "<PROJECT_NAME>"
  
  merge_bet_leg:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/merge_bet_leg.sql"
    replace_parameters:
      project: "<PROJECT_NAME>"
  
  postprocess_audit:
    queryfilepath: "/home/airflow/gcs/dags/landing_to_core_load/history_load/merge_audit_etl_core_be_bet.sql"
    replace_parameters:
      project: "<PROJECT_NAME>"
```

- **queryfilepath**: Specifies the location of the SQL file.
- **replace_parameters**: Defines runtime parameters (e.g., GCP project).

### Data Validation (validation1.yaml)

- `<validation name>`: specify .yaml file for data validation.
- `single_table_validation`: Define single table validation config.

```yaml
data_validation:
  validate_1:
    json_path: "/home/airflow/gcs/dags/landing_to_core_load/data_validation/validation1.yaml"
```

- **json_path**: Points to the validation configuration file.

#### validation1.yaml

```validation1.yaml
type: Custom-query
table_name: null
schema_name: null
target_schema_name: null
target_table_name: null
hash: '*'
custom_query_type: row
source_conn:
  source_type: BigQuery #changable field
  project_id: <PROJECT_NAME> #changable field
target_conn:
  source_type: Snowflake
  user: SNOWFLAKE  #changable field
  password: ####### #changable field
  account: THFEZAE-BE84871 #changable field
  database: SNOWFLAKE_SAMPLE_DATA/TPCH_SF1 #changable field
source_query: "SELECT run_id as C_CUSTKEY, validation_status as C_NAME FROM `<PROJECT_NAME>.pso_data_validator.results`, UNNEST(labels) AS label where label.value ='scheduled__2025-03-08T12:00:00+00:00' and validation_status = 'fail'" #changable field
target_query: "select C_NAME as C_CUSTKEY, C_NAME from TPCH_SF1.customer order by C_CUSTKEY limit 10" #changable field

calculated_fields:
  - source_calculated_columns:
      - c_custkey  #changable field
    target_calculated_columns:
      - c_custkey  #changable field
    field_alias: cast__c_custkey  #changable field
    type: cast
    depth: 0
  - source_calculated_columns:
      - c_name  #changable field
    target_calculated_columns:
      - c_name  #changable field
    field_alias: cast__c_name  #changable field
    type: cast
    depth: 0
  - source_calculated_columns: #list here all the source and table field_alias names 
      - cast__c_custkey  
      - cast__c_name  
    target_calculated_columns:
      - cast__c_custkey 
      - cast__c_name 
    field_alias: concat__all 
    type: concat
    depth: 1
  - source_calculated_columns:
      - concat__all
    target_calculated_columns:
      - concat__all
    field_alias: hash__all
    type: hash
    depth: 2
primary_keys:
  - source_column: c_custkey  #changable field
    target_column: c_custkey #changable field
    field_alias: c_custkey #changable field
    cast: null
comparison_fields:
  - source_column: hash__all
    target_column: hash__all
    field_alias: hash__all
    cast: null
```

#### General Settings
The following fields define general settings for the validation process:
- `type`: Specifies the type of process. This is set to `Custom-query` and should not be changed.
- `table_name`: Name of the source table. Can be modified.
- `schema_name`: Schema of the source table. Can be modified.
- `target_schema_name`: Schema of the target table. Can be modified.
- `target_table_name`: Name of the target table. Can be modified.
- `hash`: Defines a hash key for data validation. Default is `'*'` and should not be changed.
- `custom_query_type`: Defines the type of query execution. This is set to `row` and should not be changed.

#### Source Connection
Defines the connection details for the source database:
- `source_type`: Type of source database (e.g., `BigQuery`). Can be modified.
- `project_id`: Project ID for BigQuery source. Can be modified.

#### Target Connection
Defines the connection details for the target database:
- `source_type`: Target database type (e.g., `Snowflake`). Should not be changed.
- `user`: Snowflake username. Can be modified.
- `password`: Snowflake password. Can be modified.
- `account`: Snowflake account identifier. Can be modified.
- `database`: Target database and schema. Can be modified.

#### Queries
SQL queries used for extracting data:
- `source_query`: SQL query to extract data from the source. Can be modified. **Must return the same number of columns in the same order as the target query.**
- `target_query`: SQL query to extract data from the target. Can be modified. **Must return the same number of columns in the same order as the source query.**

#### Calculated Fields
Calculated fields define transformations applied to specific columns from the SQL queries. Each column follows this structure:

```yaml
  - source_calculated_columns:
      - column_name  # Changeable field
    target_calculated_columns:
      - column_name  # Changeable field
    field_alias: alias_name  # Changeable field
    type: cast
    depth: 0
```

#### Transformation Types:
- `depth`: Once a calculated field is defined, it can be referenced by other calculated fields at any "depth" or higher. Depth controls how many subqueries are executed in the resulting query.
- `cast`: it will cast columns if required

#### Example Configurations
1. **Casting a column**
   ```yaml
   - source_calculated_columns:
       - c_name  # Changeable field
     target_calculated_columns:
       - c_name  # Changeable field
     field_alias: cast__c_name  # Changeable field
     type: cast
     depth: 0
   ```
   This transformation applies a type cast to `c_name`.

2. **Concatenating multiple fields**
   ```yaml
   - source_calculated_columns:
       - cast__c_custkey  
       - cast__c_name  
     target_calculated_columns:
       - cast__c_custkey 
       - cast__c_name 
     field_alias: concat__all 
     type: concat
     depth: 1
   ```
   This transformation concatenates `cast__c_custkey` and `cast__c_name` into `concat__all`.

3. **Applying a hash transformation**
   ```yaml
   - source_calculated_columns:
       - concat__all
     target_calculated_columns:
       - concat__all
     field_alias: hash__all
     type: hash
     depth: 2
   ```
   This transformation hashes the concatenated value `concat__all`.

#### Adding More Columns
To add more calculated fields, follow the existing pattern under `calculated_fields` and adjust accordingly. Additional transformations can be added by modifying `source_calculated_columns`, `target_calculated_columns`, `field_alias`, `type`, and `depth`.

#### Primary Keys
Primary keys define the unique identifier for comparison.

```yaml
primary_keys:
  - source_column: c_custkey  # Changeable field
    target_column: c_custkey  # Changeable field
    field_alias: c_custkey  # Changeable field
    cast: null
```

#### Comparison Fields
Comparison fields specify the columns to be compared between source and target.

```yaml
comparison_fields:
  - source_column: hash__all
    target_column: hash__all
    field_alias: hash__all
    cast: null
```

#### Final Notes for validation yaml
- **Ensure that SQL queries return the same number of columns in the same order.**
- **Columns referenced in `calculated_fields` must exist in either `source_query` or `target_query`.**
- Ensure that connection credentials, transformation logic, and primary key definitions align with your data pipeline requirements. 

This configuration allows customization while maintaining structured data transformation and validation processes.


#### Single Table Validation

```yaml
single_table_validation:
  single_table_validation:
    table_1:
      table_details:
        project: "<PROJECT_NAME>"
        dataset: "pso_data_validator"
        table_name: "results"
        
      duplicate_check:
        key_columns: "run_id,validation_name"
        
      null_check:
        columns: "source_table_name,target_table_name,run_id"
    
    table_2:
      table_details:
        project: "<PROJECT_NAME>"
        dataset: "pso_data_validator"
        table_name: "results"
        
      duplicate_check:
        key_columns: "run_id,validation_name"
        
      null_check:
        columns: "source_table_name,target_table_name,run_id"
```

Defines validation rules for `table_1` and `table_2`, including:

- **duplicate_check**: Ensures unique primary keys.
- **null_check**: Ensures required columns are not null.

## Supporting Python Scripts

### `web_hook.py`

- Sends success/failure alerts to a webhook.

### `single_table_validation.py`

- Inserts validation results into BigQuery.
- Performs duplicate and null checks.

## Execution Steps

1. **Deploy DAG & Configurations**
   - Upload `history_load.yaml` to "config_files/" folder and `validation1.yaml` to "data_validation" folder.
2. **Trigger DAG Execution**
   - Manually trigger via UI or schedule execution.
3. **Monitor Execution**
   - Check logs and validation results in BigQuery.
4. **Receive Alerts**
   - Google Chat webhook sends alerts based on DAG success or failure.

## Conclusion

This Airflow DAG automates BQ to BQ data loading with validation checks, ensuring data integrity and compliance within the GCP environment. 🚀

