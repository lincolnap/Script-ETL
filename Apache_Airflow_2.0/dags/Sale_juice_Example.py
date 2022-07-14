from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import random 

import os

dag_path = os.getcwd()

def transform_data():
    name_prodtos = ['jugo Manzana','Jugo Pera','Jugo Tomate', 'Jugo Fresa', 'Jugo Naranja']
    name_productos = [random.choice(name_prodtos) for x in range(13000)]

    #df_name_productos = pd.DataFrame(name_productos)
    Ventas = pd.DataFrame(name_productos, columns=['Nombre Producto'])

    categorias = ['Citrico', 'Frio','Pruebas']
    df_categoria= [random.choice(categorias) for x in range(13000)]

    df_Valor =[round(random.uniform(0.54,1.20),2) for _ in range(13000)]

    Ventas['Categoria'] = df_categoria 
    Ventas['Valor'] = df_Valor

    Ventas.to_csv(r'./processed_data/ventas_random.csv',index=True)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'Sale_juice_Example',
    default_args=default_args,
    description='Ejemplo personal',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_1

