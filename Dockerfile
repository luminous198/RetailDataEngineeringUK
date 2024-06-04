FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /opt/airflow/
ADD dags /opt/airflow/dags
# ADD commons /opt/airflow/commons
ADD data_collector /opt/airflow/data_collector
ADD data_utils /opt/airflow/data_utils
ADD data-loading /opt/airflow/data-loading
ADD dbmodel /opt/airflow/dbmodel
ADD transforms /opt/airflow/transforms
ADD static_vars /opt/airflow/static_vars
ADD s3_utils /opt/airflow/s3_utils
COPY __init__.py /opt/airflow/
COPY retail-metadata/brandmeta.xlsx /opt/airflow/metadata/

USER root
RUN apt-get update && apt-get install -y gcc python3-dev


USER airflow
RUN mkdir -p ./dags ./logs ./plugins ./config

RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt