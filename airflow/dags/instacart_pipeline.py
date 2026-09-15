from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="instacart_market_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["instacart", "data-engineering"],
) as dag:

    validate_postgres = BashOperator(
        task_id="validate_postgres",
        bash_command="psql -d market_analysis -f sql/validation.sql",
    )

    run_kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="python kafka/producer.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd dbt/market_pipeline && dbt build",
    )

    validate_postgres >> run_kafka_producer >> run_dbt
