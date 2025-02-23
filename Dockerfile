FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow
RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark pyspark requests

