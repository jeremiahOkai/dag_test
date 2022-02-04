from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    "git_pull_dags",
    start_date=datetime(2022, 3, 1),
    schedule_interval="daily",
    catchup=False,
) as dag:
    git_pull = BashOperator(
        task_id="git_pull", bash_command="source /opt/airflow/gitpull.sh"
    )

    git_pull
