from airflow import DAG
from airflow.operators.python_operator import days_ago
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

from football_etl import run_football_etl