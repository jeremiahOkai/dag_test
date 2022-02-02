import os
import git
import datetime as dt

from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

#
# Stolen from R&D team :).
#


def get_latest():
    git_dir = os.environ["AIRFLOW_HOME"] + "/dags"
    g = git.cmd.Git(git_dir)
    g.pull()


default_args = {
    "owner": "dwh",
    "start_date": dt.datetime(2018, 8, 16),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "git_pull_dags",
    default_args=default_args,
    catchup=False,
    schedule_interval="*/30 * * * *",
) as dag:
    run_git_pull = PythonOperator(
        task_id="run_git_pull", python_callable=get_latest, dag=dag
    )


run_git_pull
