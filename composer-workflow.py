import datetime

import airflow
from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocClusterDeleteOperator, DataprocSubmitJobOperator
from airflow.utils import trigger_rule

default_dag_args = {
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

accident_schema = [
    {"name": "AccidentId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Distance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "NumberOfAccidents", "type": "BYTES", "mode": "REQUIRED"},
    {"name": "Severity", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "TimeId", "type": "DATE", "mode": "REQUIRED"},
    {"name": "LocationId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "SurroundingId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "TemperatureId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "VisibilityId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "WeatherConditionId", "type": "INTEGER", "mode": "REQUIRED"},
]

time_schema = [
    {"name": "TimeId", "type": "DATE", "mode": "REQUIRED"},
    {"name": "Year", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Month", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Day", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "DayOfWeek", "type": "STRING", "mode": "REQUIRED"}
]

location_schema = [
    {"name": "LocationId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Zipcode", "type": "STRING", "mode": "REQUIRED"},
    {"name": "AirportCode", "type": "STRING", "mode": "REQUIRED"},
    {"name": "City", "type": "STRING", "mode": "REQUIRED"},
    {"name": "County", "type": "STRING", "mode": "REQUIRED"},
    {"name": "State", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Street", "type": "STRING", "mode": "REQUIRED"}
]

surrounding_schema = [
    {"name": "SurroundingId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Crossing", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "Railway", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "Stop", "type": "BOOL", "mode": "REQUIRED"}
]

temperature_schema = [
    {"name": "TemperatureId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MinimumTemperature", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MaximumTemperature", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Description", "type": "STRING", "mode": "REQUIRED"}
]

visibility_schema = [
    {"name": "VisibilityId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MinimumDistance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MaximumDistance", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Description", "type": "STRING", "mode": "REQUIRED"}
]

weather_condition_schema = [
    {"name": "WeatherConditionId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Description", "type": "STRING", "mode": "REQUIRED"}
]


def generate_spark_submit_task(task_id: str, class_name: str, executor_cores: int = 1,
                               parameters: list[str] = []) -> DataprocSubmitJobOperator:
    return DataprocSubmitJobOperator(
        task_id=task_id,
        job={
            "reference": {"project_id": Variable.get("PROJECT_ID")},
            "placement": {"cluster_name": Variable.get("CLUSTER_NAME")},
            "spark_job": {
                "jar_file_utils": f"gs:/${Variable.get('BUCKET_NAME')}/us-accidents-warehouse_2.12-1.0.0.jar",
                "main_class": class_name,
                "num_executors": 1,
                "driver_memory": "512m",
                "executor_cores": executor_cores,
                "args": parameters
            }
        }
    )


def generate_big_query_table_task(task_id: str, table_id: str, schema_fields: list[dict[str, str]]):
    return BigQueryCreateEmptyTableOperator(
        task_id=task_id,
        dataset_id=Variable.get("BQ_DATASET"),
        table_id=table_id,
        schema_fields=schema_fields
    )


with models.DAG(
        dag_id="US Accidents Warehouse DAG",
        schedule_interval='@once',
        start_date=airflow.utils.dates.days_ago(1),
        default_args=default_dag_args
) as dag:
    dag.add_task(DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_config=CLUSTER_CONFIG,
        region=Variable.get("REGION"),
        cluster_name=Variable.get("CLUSTER_NAME"),
    ))
    dag.add_task(BigQueryDeleteDatasetOperator(
        task_id="delete_big_query_dataset",
        dataset_id=Variable.get("BQ_DATASET"),
        delete_contents=True
    ))
    dag.add_task(BigQueryCreateEmptyDatasetOperator(
        task_id="create_big_query_dataset",
        dataset_id=Variable.get("BQ_DATASET"),
    ))
    dag.add_tasks([
        generate_big_query_table_task('create-table-accident', 'Accident', accident_schema),
        generate_big_query_table_task('create-table-time', 'Time', time_schema),
        generate_big_query_table_task('create-table-location', 'Location', location_schema),
        generate_big_query_table_task('create-table-surrounding', 'Surrounding', surrounding_schema),
        generate_big_query_table_task('create-table-temperature', 'Temperature', temperature_schema),
        generate_big_query_table_task('create-table-visibility', 'Visibility', visibility_schema),
        generate_big_query_table_task('create-table-weather-condition', 'WeatherCondition', weather_condition_schema)
    ])
    dag.add_tasks([
        generate_spark_submit_task('load-surrounding', 'pl.michalsz.spark.SurroundingLoader', 1, [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task('load-temperature', 'pl.michalsz.spark.TemperatureLoader', 1, [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task('load-visibility', 'pl.michalsz.spark.VisibilityLoader', 1, [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task('load-weather-condition', 'pl.michalsz.spark.WeatherConditionLoader', 4,
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task('load-time', 'pl.michalsz.spark.TimeLoader', 4,
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task('load-location', 'pl.michalsz.spark.LocationLoader', 4,
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")])
    ])
    dag.add_task(
        generate_spark_submit_task('load-facts', 'pl.michalsz.spark.FactLoader', 4,
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]))
    if Variable.get("SHOULD_DELETE_CLUSTER") == 1:
        dag.add_task(DataprocClusterDeleteOperator(
            task_id='delete_dataproc',
            cluster_name=Variable.get("CLUSTER_NAME"),
            trigger_rule=trigger_rule.TriggerRule.ALL_DONE))
