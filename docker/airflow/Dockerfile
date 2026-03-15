FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN java -version
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
# Copy from docker/airflow directory (relative to build context which is project root)
COPY docker/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Add S3A JARs so spark-submit (run from this container) can access S3/MinIO.
# PySpark 3.5.0 bundles Hadoop 3.3.4 — versions must match.
RUN PYSPARK_JARS=$(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o "${PYSPARK_JARS}/hadoop-aws-3.3.4.jar" && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o "${PYSPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar"

# Bake spark jobs into image so they're always available
# (volume mount ./spark/jobs:/opt/spark-jobs can still override at runtime)
USER root
RUN mkdir -p /opt/spark-jobs
COPY spark/jobs/ /opt/spark-jobs/
RUN chown -R airflow:root /opt/spark-jobs
USER airflow