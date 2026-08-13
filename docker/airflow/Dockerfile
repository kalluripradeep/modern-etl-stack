FROM apache/airflow:3.3.0-python3.11

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

# Bake every JAR the Spark jobs need into the image, at build time.
#
# These used to be fetched per run via --conf spark.jars.packages, which meant
# every single Spark task resolved ~1GB from Maven Central into the pod before
# doing any work. That is slow, and it fails outright the moment the cluster
# cannot reach repo1.maven.org:
#   :: UNRESOLVED DEPENDENCIES ::
#   module not found: org.apache.iceberg#iceberg-spark-runtime-3.5_2.12;1.4.2
# It was also what filled the node's ephemeral storage in the incident that
# led to #127 -- each retained pod was holding its own copy of that download.
#
# Baking them in makes runs hermetic: no network at submit time, nothing to
# re-download, and the versions are pinned in one place instead of a string
# inside a DAG. PySpark 3.5.0 bundles Hadoop 3.3.4, so hadoop-aws must match
# it exactly; iceberg-spark-runtime must match Spark 3.5 / Scala 2.12.
RUN PYSPARK_JARS=$(python -c "import pyspark; import os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o "${PYSPARK_JARS}/hadoop-aws-3.3.4.jar" && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o "${PYSPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar" && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
        -o "${PYSPARK_JARS}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" && \
    curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
        -o "${PYSPARK_JARS}/postgresql-42.7.4.jar"

# Bake spark jobs into image so they're always available
# (volume mount ./spark/jobs:/opt/spark-jobs can still override at runtime)
USER root
RUN mkdir -p /opt/spark-jobs
COPY spark/jobs/ /opt/spark-jobs/
RUN chown -R airflow:root /opt/spark-jobs

# /opt/spark-jars holds the two JARs the EXECUTORS also need. pyspark/jars is
# already on the driver's classpath, but executors run on the Spark workers,
# whose image ships hadoop-aws and the AWS SDK and knows nothing about Iceberg
# or Postgres. Passing these via --jars has the driver serve them to every
# executor, so the worker image needs no change. A stable path keeps the DAG
# from having to locate site-packages at runtime. Done here because /opt is
# only writable as root.
RUN PYSPARK_JARS=$(su airflow -s /bin/bash -c "python -c \"import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))\"") && \
    mkdir -p /opt/spark-jars && \
    cp "${PYSPARK_JARS}/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" \
       "${PYSPARK_JARS}/postgresql-42.7.4.jar" /opt/spark-jars/ && \
    chown -R airflow:root /opt/spark-jars
USER airflow