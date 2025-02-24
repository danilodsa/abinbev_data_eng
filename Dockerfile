FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk wget && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
RUN tar -xvzf spark-3.2.0-bin-hadoop3.2.tgz
RUN mv spark-3.2.0-bin-hadoop3.2 /opt/spark

ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
ENV SPARK_HOME /opt/spark
ENV PATH="/opt/spark/bin:${PATH}"

USER airflow
RUN pip install apache-airflow==2.7.1 apache-airflow-providers-apache-spark pyspark requests
USER root
