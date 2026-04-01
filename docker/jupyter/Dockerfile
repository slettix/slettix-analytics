FROM apache/spark:3.5.8-python3

USER root

# Install JupyterLab and the Delta Lake Python bindings
RUN pip install --no-cache-dir \
      jupyterlab==4.* \
      delta-spark==3.2.0 \
      deltalake==0.21.0 \
      duckdb>=1.1.0 \
      pandas \
      numpy \
      requests \
      pyarrow==17.0.0 \
      matplotlib \
      seaborn \
      scipy

# Download the same JARs as the Spark cluster image
RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

ENV HOME=/home/spark

RUN mkdir -p /home/spark/notebooks && chown -R spark /home/spark

EXPOSE 8888

WORKDIR /home/spark/notebooks

USER spark

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--IdentityProvider.token=''", \
     "--ServerApp.password=''"]
