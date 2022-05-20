import datetime

import airflow
from airflow import models
from airflow.models import Variable
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
                "packages": "io.delta:delta-core_2.12:1.0.0",
                "conf": {
                    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
                },
                "args": parameters
            }
        }
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
    dag.add_task(generate_spark_submit_task('create-table', 'pl.michalsz.spark.CreateTable', [Variable.get("DATABASE_LOCALISATION")]))
    dag.add_tasks([
        generate_spark_submit_task('load-surrounding', 'pl.michalsz.spark.SurroundingLoader'),
        generate_spark_submit_task('load-temperature', 'pl.michalsz.spark.TemperatureLoader'),
        generate_spark_submit_task('load-visibility', 'pl.michalsz.spark.VisibilityLoader'),
        generate_spark_submit_task('load-weather-condition', 'pl.michalsz.spark.WeatherConditionLoader',
                                  [Variable.get("INPUT_PATH")], 4),
        generate_spark_submit_task('load-time', 'pl.michalsz.spark.TimeLoader', [Variable.get("INPUT_PATH")], 4),
        generate_spark_submit_task('load-location', 'pl.michalsz.spark.LocationLoader', [Variable.get("INPUT_PATH")], 4),
    ])
    dag.add_task(generate_spark_submit_task('load-facts', 'pl.michalsz.spark.FactLoader', [Variable.get("INPUT_PATH")], 4))
    if (Variable.get("SHOULD_DELETE_CLUSTER") == 1):
        dag.add_task(DataprocClusterDeleteOperator(
            task_id='delete_dataproc',
            cluster_name=Variable.get("CLUSTER_NAME"),
            trigger_rule=trigger_rule.TriggerRule.ALL_DONE))
