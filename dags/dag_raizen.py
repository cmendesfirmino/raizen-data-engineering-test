from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.transform_data import transform
from scripts.upload_s3 import upload_files
from scripts.write_to_dw import write_to_postgres


default_args = {
    'owner':'Cristian Firmino',
    'depends_on_past': False,
    'email': ['cmendesfirmino@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
}

description = 'This dag is to extract pivot cache from XLS files.'

@dag(default_args=default_args, schedule_interval='@once', description=description,start_date=datetime(2022,1,23))
def raizen_test():

#download data and convert to libreoffice, both using bashoperators, reading shell scripts.
    download_data = BashOperator(
        task_id='download_data',
        bash_command="scripts/download_data.sh")

    convert_file = BashOperator(
        task_id='convert_file',
        bash_command='scripts/convert_file.sh'
    )

#python operator to transform data, unpivot and save as parquet file.
    @task
    def transform_data():
        return transform()

#upload result to s3 bucket and persist to postgresql db
    @task
    def upload_to_s3(filepath):
        upload_files(filepath)
    
    @task
    def write_to_dw(filepath):
        write_to_postgres(filepath)    
    
    download_data >> convert_file
    py_transform = transform_data()
    convert_file.set_downstream(py_transform)

    #read parallel these two tasks, using the output from transform data (directory)
    upload_s3 = upload_to_s3(py_transform)
    persist_dw = write_to_dw(py_transform)
    
raizen_test = raizen_test()