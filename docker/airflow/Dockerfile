FROM apache/airflow:2.9.2

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
    great-expectations>=1.0.0 \
    deltalake>=0.17.0
