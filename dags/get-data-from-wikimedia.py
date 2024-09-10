from airflow import DAG # type: ignore
from datetime import datetime
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from urllib import request
import os 

airflow_home = os.environ['AIRFLOW_HOME']


def get_data(**kwargs):
    year = kwargs["year"]
    month = kwargs["month"]
    day = kwargs["day"]
    hour = kwargs["hour"]
    output_path = kwargs["output_path"]
    
    url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
     
    request.urlretrieve(url, output_path)
    
    
def fetch_data(**kwargs):
    pagenames = kwargs["pagenames"]
    fetch_path = kwargs["fetch_path"]
    result = dict.fromkeys(pagenames, 0)
    path_sql_execute = kwargs["path_sql_execute"]
    _date = kwargs["_date"]
    
    with open(fetch_path, 'r') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
                
    
    with open(path_sql_execute, 'w') as f:
        for pagename, pageviewcounts in result.items():
            f.write(
                "Insert into pageview_counts values ('" + pagename + "', " + pageviewcounts + ", '" + _date + "');\n"
            )
 
    
with DAG(
    dag_id='wikimedia_analyse',
    start_date=datetime(2024, 7, 20),
    end_date=datetime(2024, 7, 21),
    schedule_interval='@hourly',
    template_searchpath='/opt/airflow/data/'
) as dag:
    
    data_wikimedia_from_pythondocs = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_kwargs={
            "year": "{{execution_date.year}}",
            "month": "{{execution_date.month}}",
            "day": "{{execution_date.day}}",
            "hour": "{{execution_date.hour}}",
            "output_path": "/opt/airflow/data/wikipageviews.gz"  
 
        }  
    )
    
  
    extract_gz = BashOperator(
        task_id='extract_gz',
        bash_command='gunzip --force /opt/airflow/data/wikipageviews.gz'  
    )
    
    create_table_postgres = PostgresOperator(
        task_id='create_table_postgres',
        postgres_conn_id='postgres_conn',
        sql="""
            Create table if not exists pageview_counts (
                pagename varchar(50) not null,
                pageviewcount int not null,
                datatime Timestamp not null
            )
        """
    )
    
    fetch_data_into_postgres = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={
            "pagenames": ["Google", "Amazon", "Microsoft", "Apple", "Facebook"],
            "fetch_path": "/opt/airflow/data/wikipageviews",
            "path_sql_execute": "/opt/airflow/data/insert_query.sql",
            "_date": "{{execution_date}}"
 
        }
    )
    
    write_to_postgres = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='postgres_conn',
        sql='insert_query.sql'
    )
    
    s3_copy_operator = BashOperator(
        task_id='s3_copy_to_bucket',
        bash_command=f'python {airflow_home}/scripts/S3-transfer.py'
    )
    
    
data_wikimedia_from_pythondocs >> extract_gz >> create_table_postgres >> fetch_data_into_postgres >> write_to_postgres >> s3_copy_operator

 






