FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Kafka + JDBC (StarRocks MySQL-protocol) connector jars, pinned to versions
# compatible with Spark 3.5.1 / Scala 2.12. Baked into the image at build
# time so the streaming job never needs network access to resolve
# dependencies at runtime (--packages would re-download on every restart).
ARG SPARK_JARS_DIR=/opt/spark/jars
RUN curl -fsSL -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fsSL -o ${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o ${SPARK_JARS_DIR}/mysql-connector-j-8.3.0.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar

COPY spark/ ./spark/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

# Pre-create the checkpoint directory and own it as the non-root spark user
# (185) *before* it's mounted as a named volume - Docker seeds a new named
# volume from whatever already exists at that path in the image, ownership
# included. Without this, the empty volume is created root-owned and the
# container (running as 185) fails with "mkdir ... failed" the moment
# Structured Streaming tries to create its checkpoint subdirectories.
RUN mkdir -p /opt/spark-checkpoints && chown -R 185:185 /app /opt/spark-checkpoints
USER 185

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/app/spark/streaming_job.py"]
