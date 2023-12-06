from datetime import timedelta
from email.policy import default
from sched import scheduler

from airflow import DAG

from airflow.operators.python import pythonOperator
from airflow.utils.dates import days_ago

import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@examples.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}


def screpe():
    logging.info("perfoming scraping")

def process():
    logging.info("perfoming scraping")

def save():
    logging.info("perfoming scraping")


with DAG(
    'first',
    default_args=default_args,
    description = 'A simple',
    schedule_interval= timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],

)as dag:
    screpe_task = pythonOperator(task_id = "scrape", python_callable=screpa)
    process_task = pythonOperator(task_id = "process", python_callable=process)
    save_task = pythonOperator(task_id = "save", python_callable=save)

    screpe_task >> process_task >> save_task
