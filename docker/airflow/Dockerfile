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

# Bake spark jobs into image so they're always available
# (volume mount ./spark/jobs:/opt/spark-jobs can still override at runtime)
USER root
RUN mkdir -p /opt/spark-jobs
COPY spark/jobs/ /opt/spark-jobs/
RUN chown -R airflow:root /opt/spark-jobs
USER airflow