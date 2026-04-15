FROM apache/spark:3.5.8-python3

USER root

# ── Delta Lake + S3A JARs ─────────────────────────────────────────────────────
RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ── Kafka JARs (Structured Streaming → Kafka) ─────────────────────────────────
# På Kubernetes Operator kan ikke --packages brukes ved runtime — JARs må ligge i imaget.
# Fire JARs er nødvendig for spark.readStream.format("kafka"):
#   1. spark-sql-kafka-0-10   — hoveddriveren for Kafka structured streaming
#   2. spark-token-provider   — autentiseringstoken-provider (transitiv avhengighet)
#   3. kafka-clients           — Kafka-klientbiblioteket
#   4. commons-pool2           — connection-pool (avhengighet av kafka-clients)
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ── Python-bindings ───────────────────────────────────────────────────────────
RUN pip install --no-cache-dir delta-spark==3.2.0 kafka-python>=2.0.0

# ── Jobs og conf bakt inn (hostPath fungerer ikke i multi-node kind cluster) ──
# Build context må være repo-roten (se build-spark-image.sh)
COPY jobs/ /opt/spark/jobs/
COPY conf/  /opt/spark/conf/

USER spark
