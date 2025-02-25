FROM apache/airflow:2.10.0
USER root
RUN apt-get update && apt-get install -y python3.11
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

COPY ./requirements.txt /requirements.txt

USER airflow
RUN pip install -r /requirements.txt