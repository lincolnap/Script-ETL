from datetime import datetime, timedelta
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago

import pandas as pd
import random
import sqlalchemy as sql


conn = BaseHook.get_connection('mssql')

def transform_data():
    name_prodtos = ['jugo Manzana','Jugo Pera','Jugo Tomate', 'Jugo Fresa', 'Jugo Naranja']
    name_productos = [random.choice(name_prodtos) for x in range(50000)]

    #df_name_productos = pd.DataFrame(name_productos)
    Ventas = pd.DataFrame(name_productos, columns=['Nombre Producto'])

    categorias = ['Citrico', 'Frio','Pruebas']
    df_categoria= [random.choice(categorias) for x in range(50000)]

    df_Valor =[round(random.uniform(0.54,1.20),2) for x in range(50000)]

    hora = [datetime.now(pytz.timezone('America/Bogota')) for x in range(50000)]

    Ventas['Categoria'] = df_categoria 
    Ventas['Valor'] = df_Valor
    Ventas['Dia_Hora'] = hora

    Ventas.to_csv(r'./raw_data/ventas_random.csv',index=False)   

def load_data():
    csv1 = pd.read_csv(r'./raw_data/ventas_random.csv')
    # use the connection for hidden the user and password 
    engine = sql.create_engine('mssql+pymssql://' + str(conn.login)+':'+ str(conn.password) +'@192.168.3.24:1433/Data_hub') 
    csv1.to_sql('airflows_test',con=engine,if_exists='replace')

def check():
    conn = BaseHook.get_connection('mssql')
    print(str(conn.get_extra()))
    

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2022, 6, 20),
    'end_date': datetime(2022, 6, 23)
    # 'email': ['lincoln0694@gmail.com']
    # 'on_success_callback': some_other_function #si se completa la tarea
    # 'on_failure_callback': some_function #No se completa la tarea

}

ingestion_dag = DAG(
    'Ejemplo_2',
    default_args=default_args,
    description='Carga de datos de csv to SQL server 2',
    schedule_interval= "*/10 * * * *",
    catchup=False,
    tags=["Connections","Sql Server"]
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)
delay = BashOperator(
    task_id="delay_bash_task", 
    dag=ingestion_dag,
    bash_command="sleep 3"
)
task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)
task_3 = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=ingestion_dag,
)

task_1 >> delay >> [task_2, task_3]

