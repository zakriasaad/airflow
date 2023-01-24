from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from datetime import datetime, timedelta

default_dag_args={
    'start_date':datetime(2023,2,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}
with DAG("exercise1", schedule_interval=None, default_args= default_dag_args) as dag:

    task_0=BashOperator(task_id='bash_task', bash_command= "echo 'Command executed from bash operator' ")
    task_1=BashOperator(task_id='bash_file_move', bash_command= r"cp C:\Users\zakria\Desktop\raw_data\text.txt C:\Users\zakria\Desktop\clean_data")
    task_2=BashOperator(task_id='bash_remove_file',bash_command=r"rm C:\Users\zakria\Desktop\clean_data\text.txt")
    
    task_0>>task_1>>task_2