FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update && apt-get install -y gcc python3-dev


USER airflow
RUN mkdir -p ./dags ./logs ./plugins ./config

RUN echo -e "AIRFLOW_UID=$(id -u)" > .env

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt