from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
import json
import time
from airflow.utils.dates import days_ago

default_args={
    'owner':'airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

create_query="""
DROP TABLE IF EXISTS employee
CREATE TABLE employee(name VARCHAR(256) , age INT NOT NULL)
"""
insert_data_query="""
INSERT INTO employee(name,age)
values(Zakria,26),(David,30),(Mark,27)
"""
calculate_avg_age="""
SELECT AVG(age)
FROM employee;
"""


dag_postgres=DAG(dag_id='postgres_dag_connection',default_args=default_args, schedule_interval=None,start_date=days_ago(1))

create_table=PostgresOperator(task_id='creation_of_table',sql=create_query,
                              dag=dag_postgres,postgres_conn_id='postgres_zakria_local')
insert_data=PostgresOperator(task_id='insertion_of_data',sql=insert_data_query,
                              dag=dag_postgres,postgres_conn_id='postgres_zakria_local')
avg_data=PostgresOperator(task_id='creating_grouped_table',sql=calculate_avg_age,
                              dag=dag_postgres,postgres_conn_id='postgres_zakria_local')

create_table>>create_query>>avg_data