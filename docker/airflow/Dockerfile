FROM apache/airflow:2.9.2-python3.11

USER root

# Java is required for spark-submit (comes with pyspark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/local/java

ENV JAVA_HOME=/usr/local/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.8 \
    delta-spark==3.2.0 \
    apache-airflow-providers-apache-spark==4.* \
    apache-airflow-providers-cncf-kubernetes>=8.0.0 \
    apache-airflow-providers-papermill==1.* \
    great-expectations>=1.0.0 \
    deltalake>=0.17.0 \
    papermill>=2.6.0 \
    requests>=2.31.0 \
    ipykernel>=6.0.0 \
    pyarrow==17.0.0 \
    kafka-python>=2.0.0 \
    cryptography>=42.0.0

RUN python -m ipykernel install --user --name python3 --display-name "Python 3"
