from airflow import DAG # type: ignore
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
import pandas as pd
import psycopg2
import os
from psycopg2 import sql
import numpy as np

def conectar_ao_postgresql():
    try: 
        conexao = psycopg2.connect(
                host="host.docker.internal",  
                database="backup_northwind",
                user="northwind_user", 
                password="thewindisblowing", 
                port="5433"
            )
        conexao.close()
        return True 
    except:
        return False

def validar_conexao(ti):
    conexao_valida = ti.xcom_pull(task_ids='conectaBD')
    if conexao_valida:
        return 'copia'
    else:
        return 'invalido'


def update_or_create_table(conexao, table_name, parquet_file):
    try:
        df = pd.read_parquet(parquet_file)
        
        cursor = conexao.cursor()
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        exists = cursor.fetchone()[0]
        
        if exists:
            print(f"Tabela {table_name} já existe. Atualizando dados...")
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            for index, row in df.iterrows():
                columns = ', '.join(df.columns)
                values = ', '.join([f"'{escape_sql_value(value)}'" if isinstance(value, str) else str(escape_sql_value(value)) for value in row])
                cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        else:
            print(f"Tabela {table_name} não existe. Criando a tabela...")
            columns_with_types = ", ".join([f"{col} {get_postgres_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
            cursor.execute(f"CREATE TABLE {table_name} ({columns_with_types});")
            for index, row in df.iterrows():
                columns = ', '.join(df.columns)
                values = ', '.join([f"'{escape_sql_value(value)}'" if isinstance(value, str) else str(escape_sql_value(value)) for value in row])
                cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        
        conexao.commit()
        cursor.close()
        
    except Exception as e:
        print(f"Erro ao atualizar ou criar tabela {table_name}: {e}")
        conexao.rollback()

# Converte os tipos padroes do pandas para tipo do postgres 
def get_postgres_type(dtype):
    """Retorna o tipo correspondente no PostgreSQL para o tipo do pandas"""
    if dtype == 'int64':
        return 'BIGINT'
    elif dtype == 'float64':
        return 'DOUBLE PRECISION'
    elif dtype == 'object':
        return 'TEXT'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'TEXT'   
    
# Ajusta valores não aceitos pelo SQL
def escape_sql_value(value):
    
    if isinstance(value, str):
        return value.replace("'", "''")
    elif isinstance(value, float) and np.isnan(value):
        return 'NULL'
    elif value is None:
        return 'NULL'  
    else:
        return value  
        
def CopiaParquetBanco():
    
    try:
        conexao = psycopg2.connect(
                host="host.docker.internal",  
                database="backup_northwind",
                user="northwind_user", 
                password="thewindisblowing", 
                port="5433"
            )
        
        pasta_ini = "/data/postgres/"
        data_atual = datetime.today().strftime('%Y-%m-%d')
        
        for root, dirs, files in os.walk(pasta_ini):
            for file in files:
                if file.endswith(".parquet"):
                    
                    diretorio_data = root.split(os.sep)[-1]
                    
                    if  diretorio_data == data_atual:
                        
                        table_name = root.split(os.sep)[-2]
                        parquet_file = os.path.join(root, file)
                        print(f"Table: {table_name} -- parquetfile: {parquet_file}")
                        update_or_create_table(conexao, table_name, parquet_file)
            
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def copiaCSVBanco():
    try:
        conexao = psycopg2.connect(
                host="host.docker.internal",  
                database="backup_northwind",
                user="northwind_user", 
                password="thewindisblowing", 
                port="5433"
            )
        data_atual = datetime.today().strftime('%Y-%m-%d')
        table_name = "order_details"
        parquet_file = f"/data/csv/order_details/{data_atual}/file.parquet"
        print(f"Table: {table_name} -- parquetfile: {parquet_file}")
        update_or_create_table(conexao, table_name, parquet_file)
        return True
    except Exception as e:
        return False

with DAG('etapa2_dag', description="Processo de salvar no banco etapa 2", start_date = datetime(2025,1,1), schedule_interval = timedelta(days=1), catchup=False, tags = ['PS','et2']) as dag:
    
    conectaBD = PythonOperator(task_id = 'conectaBD', python_callable = conectar_ao_postgresql)
    validar = BranchPythonOperator(task_id='validar',python_callable=validar_conexao)
    copia = PythonOperator(task_id = 'copia', python_callable = CopiaParquetBanco)
    invalido = BashOperator(task_id = 'invalido', bash_command = "echo 'Error'")
    copia_csv = PythonOperator(task_id = 'copia_csv', python_callable = copiaCSVBanco)
    
    conectaBD >> validar >> [copia,invalido]
    copia >> copia_csv
