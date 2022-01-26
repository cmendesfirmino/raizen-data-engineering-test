import datetime
from airflow.decorators import dag, task
from datetime import datetime
from prd.scripts.write_to_dw import write_to_postgres

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

#set default args airflow
default_args = {
    'owner': 'Cristian Firmino',
    'depends_on_past': False,
    'start_date': datetime(2021,8,4,21,10),
    'email': 'cmendesfirmino@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False
}
#create dag
@dag(default_args=default_args, schedule_interval=None, \
    description="ETL de dados do IBGE para Data Lake e DW", tags=['aws', 'postgres'])
def desafio_final_etl():
    @task
    def write_to_dw(filename):
        write_to_postgres(filename)

    file_name = './data/stage/vendas_combustivel.xls'
    write_to_dw=write_to_dw(file_name)
desafio_final_etl=desafio_final_etl()