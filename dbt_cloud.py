from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
import datetime, json

default_args = {"start_date": datetime.datetime(2021, 1, 1)}
dbt_header = {
    "Content-Type": "application/json",
    "Authorization": "Token d39ab9356c3062ee12ec5c972fcb5211ade9c085",
}


def getDbtMessage(message):
    return {"cause": message}


def getDbtApiLink(jobId, accountId):
    return "accounts/{0}/jobs/{1}/run/".format(accountId, jobId)


def getDbtApiOperator(task_id, jobId, message="Triggered by Airflow", accountId=44119):
    return SimpleHttpOperator(
        task_id=task_id,
        method="POST",
        data=json.dumps(getDbtMessage(message)),
        http_conn_id="dbt_api",
        endpoint=getDbtApiLink(jobId, accountId),
        headers=dbt_header,
    )


with DAG(
    "dbt_cloud_test",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    load_user_cloud = getDbtApiOperator("load_users", 54684)
