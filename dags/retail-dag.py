import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.insert(0,parent_path)


from data_collector.aldi_collector import scrape_aldi
from data_collector.asda_collector import scrape_asda
from data_collector.morrissons_collector import scrape_morrissons
from transforms.create_datafile import create_datafile
from transforms.data_cleanup import data_cleanup
from temp1 import a
from datetime import timedelta
from s3_utils.s3_connection import upload_clean_file_to_s3


dag_start_date_time = datetime.now()


default_args = {
    'owner': 'Milan',
    'start_date': datetime(2023, 10, 22)
}

file_date_to_get = str(datetime.now().date())

dag = DAG(
    dag_id='retail_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl', 'pipeline']
)

# extraction from aldi
aldi_extract = PythonOperator(
    task_id='aldi_collector',
    python_callable=scrape_aldi,
    retries=5,
    op_kwargs={
        "retry_delay": timedelta(seconds=30)
    },
    dag=dag
)

# extraction from asda
asda_extract = PythonOperator(
    task_id='asda_collector',
    python_callable=scrape_asda,
    retries=5,
    op_kwargs={
        "retry_delay": timedelta(seconds=30)
    },
    dag=dag
)

# extraction from morrissons
morrissons_extract = PythonOperator(
    task_id='morrissons_collector',
    python_callable=scrape_morrissons,
    retries=5,
    op_kwargs={
        "retry_delay": timedelta(seconds=30)
    },
    dag=dag
)

datafile_creation = PythonOperator(
    task_id='datafile_creation',
    python_callable=create_datafile,
    op_kwargs={
        "date_to_get": file_date_to_get
    },
    dag=dag
)

datafile_cleanup = PythonOperator(
    task_id='datafile_cleanup',
    python_callable=data_cleanup,
    op_kwargs={
        "date_to_get": file_date_to_get
    },
    dag=dag
)



# upload to s3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_clean_file_to_s3,
    dag=dag
)

[morrissons_extract, asda_extract, aldi_extract]  >> datafile_creation >> datafile_cleanup >> upload_s3
