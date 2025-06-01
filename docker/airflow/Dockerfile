FROM apache/airflow:3.0.1-python3.11

ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install -y python3-pip

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

USER $AIRFLOW_UID