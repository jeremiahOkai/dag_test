from datetime import datetime
import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "dwh",
    "start_date": datetime(2018, 8, 16),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "git_pull_dags_new",
    default_args=default_args
    schedule_interval="*/30 * * * *",
    catchup=False
) as dag:
    git_pull = BashOperator(
        task_id="git_pull", bash_command="source /opt/airflow/gitpull.sh"
    )

    git_pull
