import datetime
import logging
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def greet():
    logging.info("hello World")

def current_time():
    logging.info("current time is: {datetime.datetime.now()}")

def working_dir():
    logging.info("current working Directory is: {os.getcwd()}")

def complete():
    logging.info("Program is complete")

dag = DAG('lesson1.Demo12',start_date= datetime.datetime.now() -datetime.timedelta(days=60), schedule_interval="@hourly")
greet_task = PythonOperator (task_id="greet_task",python_callable=greet,dag=dag)
current_time_task=PythonOperator(task_id="current_time",python_callable=current_time,dag=dag)
working_dir_task=PythonOperator(task_id="working_dir",python_callable=working_dir,dag=dag)
complete_task=PythonOperator(task_id="complete",python_callable=complete,dag=dag)

# defining dependency
greet_task  >> current_time_task
greet_task >> working_dir_task
current_time_task >> complete_task
working_dir_task >> complete_task