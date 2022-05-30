import datetime

import airflow
from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator

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
    {"name": "Distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "NumberOfAccidents", "type": "BYTES", "mode": "NULLABLE"},
    {"name": "Severity", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "TimeId", "type": "DATE", "mode": "NULLABLE"},
    {"name": "LocationId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "SurroundingId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "TemperatureId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "VisibilityId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "WeatherConditionId", "type": "INTEGER", "mode": "REQUIRED"},
]

time_schema = [
    {"name": "TimeId", "type": "DATE", "mode": "NULLABLE"},
    {"name": "Year", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "Month", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "Day", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DayOfWeek", "type": "STRING", "mode": "NULLABLE"}
]

location_schema = [
    {"name": "LocationId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Zipcode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "AirportCode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "City", "type": "STRING", "mode": "NULLABLE"},
    {"name": "County", "type": "STRING", "mode": "NULLABLE"},
    {"name": "State", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Street", "type": "STRING", "mode": "NULLABLE"}
]

surrounding_schema = [
    {"name": "SurroundingId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Crossing", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "Railway", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "Stop", "type": "BOOL", "mode": "REQUIRED"}
]

temperature_schema = [
    {"name": "TemperatureId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MinimumTemperature", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "MaximumTemperature", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "Description", "type": "STRING", "mode": "NULLABLE"}
]

visibility_schema = [
    {"name": "VisibilityId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "MinimumDistance", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "MaximumDistance", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "Description", "type": "STRING", "mode": "NULLABLE"}
]

weather_condition_schema = [
    {"name": "WeatherConditionId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Description", "type": "STRING", "mode": "NULLABLE"}
]


def generate_spark_submit_task(dag, task_id: str, class_name: str, parameters = []) -> DataprocSubmitJobOperator:
    return DataprocSubmitJobOperator(
        dag=dag,
        task_id=task_id,
        region=Variable.get("REGION"),
        job={
            "reference": {"project_id": Variable.get("PROJECT_ID")},
            "placement": {"cluster_name": Variable.get("CLUSTER_NAME")},
            "spark_job": {
                "jar_file_uris": [f"gs://{Variable.get('BUCKET_NAME')}/us-accidents-warehouse_2.12-1.0.0.jar",
                                  "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                                  f"gs://{Variable.get('BUCKET_NAME')}/spark-alchemy-assembly-1.1.0.jar"],
                "main_class": class_name,
                "args": parameters
            }
        }
    )


def generate_big_query_table_task(dag, task_id: str, table_id: str, schema_fields):
    return BigQueryCreateEmptyTableOperator(
        dag=dag,
        task_id=task_id,
        dataset_id=Variable.get("BQ_DATASET"),
        table_id=table_id,
        schema_fields=schema_fields
    )


with models.DAG(
        dag_id="US_Accidents_Warehouse_DAG",
        schedule_interval='@once',
        start_date=airflow.utils.dates.days_ago(1),
        default_args=default_dag_args
) as dag:
    create_cluster_operation = DataprocCreateClusterOperator(
        dag=dag,
        task_id="create_cluster",
        cluster_config=CLUSTER_CONFIG,
        region=Variable.get("REGION"),
        cluster_name=Variable.get("CLUSTER_NAME"),
    )
    delete_big_query_dataset_operation = BigQueryDeleteDatasetOperator(
        dag=dag,
        task_id="delete_big_query_dataset",
        dataset_id=Variable.get("BQ_DATASET"),
        project_id=Variable.get("PROJECT_ID"),
        delete_contents=True
    )
    create_big_query_dataset_operation = BigQueryCreateEmptyDatasetOperator(
        dag=dag,
        task_id="create_big_query_dataset",
        trigger_rule='all_done',
        dataset_id=Variable.get("BQ_DATASET"),
        project_id=Variable.get("PROJECT_ID"),
        location=Variable.get("REGION")
    )
    create_tables_operation = [
        generate_big_query_table_task(dag, 'create-table-accident', 'Accident', accident_schema),
        generate_big_query_table_task(dag, 'create-table-time', 'Time', time_schema),
        generate_big_query_table_task(dag, 'create-table-location', 'Location', location_schema),
        generate_big_query_table_task(dag, 'create-table-surrounding', 'Surrounding', surrounding_schema),
        generate_big_query_table_task(dag, 'create-table-temperature', 'Temperature', temperature_schema),
        generate_big_query_table_task(dag, 'create-table-visibility', 'Visibility', visibility_schema),
        generate_big_query_table_task(dag, 'create-table-weather-condition', 'WeatherCondition', weather_condition_schema)
    ]

    load_dimensions_operation = [
        generate_spark_submit_task(dag, 'warehouse_load-surrounding', 'SurroundingLoader', [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task(dag, 'warehouse_load-temperature', 'TemperatureLoader', [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task(dag, 'warehouse_load-visibility', 'VisibilityLoader', [Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task(dag, 'warehouse_load-weather-condition', 'WeatherConditionLoader',
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task(dag, 'warehouse_load-time', 'TimeLoader',
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")]),
        generate_spark_submit_task(dag, 'warehouse_load-location', 'LocationLoader',
                                   [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")])
    ]
    load_facts_operation = generate_spark_submit_task(dag, 'warehouse_load-facts', 'FactLoader',
                                                      [Variable.get("INPUT_PATH"), Variable.get("TEMPORARY_BUCKET"), Variable.get("BQ_DATASET")])

    delete_cluster_operation = DataprocDeleteClusterOperator(
        dag=dag,
        region=Variable.get("REGION"),
        task_id='delete_dataproc',
        cluster_name=Variable.get("CLUSTER_NAME"),
        trigger_rule='all_done'
    )

    create_cluster_operation >> delete_big_query_dataset_operation >> create_big_query_dataset_operation >> create_tables_operation
    create_tables_operation[0] >> load_dimensions_operation >> load_facts_operation >> delete_cluster_operation
    create_tables_operation[1] >> load_dimensions_operation[4]
    create_tables_operation[2] >> load_dimensions_operation[5]
    create_tables_operation[3] >> load_dimensions_operation[0]
    create_tables_operation[4] >> load_dimensions_operation[1]
    create_tables_operation[5] >> load_dimensions_operation[2]
    create_tables_operation[6] >> load_dimensions_operation[3]
