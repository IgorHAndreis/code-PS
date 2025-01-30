from airflow import DAG # type: ignore
from datetime import datetime,timedelta

from airflow.operators.python import PythonOperator,BranchPythonOperator # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

import pandas as pd

import psycopg2

from sqlalchemy import create_engine
import os

def conectar_ao_postgresql():
    try: 
        conexao = psycopg2.connect(
                host="host.docker.internal",  
                database="northwind",
                user="northwind_user", 
                password="thewindisblowing" 
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
    
def CopiaBancoParquet():
    try:
        engine = create_engine("postgresql://northwind_user:thewindisblowing@host.docker.internal/northwind")
        data_atual = datetime.today().strftime('%Y-%m-%d')
        diretorio_base = "/data/postgres/"
        
        with engine.connect() as conexao:
            query_tabelas = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """
            tabelas = pd.read_sql(query_tabelas, conexao)
        
        for _, row in tabelas.iterrows():
            nome_tabela = row['table_name']
            caminho_tabela = os.path.join(diretorio_base, nome_tabela, data_atual)
            os.makedirs(caminho_tabela, exist_ok=True)
            
            query_dados = f"SELECT * FROM {nome_tabela};"
            with engine.connect() as conexao:
                df = pd.read_sql(query_dados, conexao)
            
            print(f"Salvando dados da tabela {nome_tabela} no arquivo {caminho_tabela}/file.parquet")
            caminho_arquivo = os.path.join(caminho_tabela, "file.parquet")
            df.to_parquet(caminho_arquivo, index=False)
            
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return
    
def copiaCSVParquet():
    
    caminho = "/data/order_details.csv"
    
    if not os.path.exists(caminho):
        return "Erro"
    
    df = pd.read_csv(caminho)
    data_atual = datetime.today().strftime('%Y-%m-%d')
    diretorio_base = "/data/csv/"
    destino = os.path.join(diretorio_base, "order_details", data_atual)
    
    os.makedirs(destino, exist_ok=True)
    
    nome_arquivo = "file.parquet"
    detino_parquet = os.path.join(destino, nome_arquivo)
    
    df.to_parquet(detino_parquet, index=False)
    

with DAG('etapa1_dag', description="Processo de extracao etapa 1", start_date = datetime(2025,1,1), schedule_interval = timedelta(days=1), catchup=False, tags = ['PS','et1']) as dag:
    
    conectaBD = PythonOperator(task_id = 'conectaBD', python_callable = conectar_ao_postgresql)
    validar = BranchPythonOperator(task_id='validar',python_callable=validar_conexao)
    copia = PythonOperator(task_id = 'copia', python_callable = CopiaBancoParquet)
    invalido = BashOperator(task_id = 'invalido', bash_command = "echo 'Error'")
    copia_csv = PythonOperator(task_id = 'copia_csv', python_callable = copiaCSVParquet)
    
    conectaBD >> validar >> [copia,invalido]
    copia >> copia_csv