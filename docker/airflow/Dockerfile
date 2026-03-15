FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && JAVA_REAL=$(readlink -f /usr/bin/java) \
    && JAVA_HOME_ACTUAL=$(dirname $(dirname $JAVA_REAL)) \
    && ln -sfn $JAVA_HOME_ACTUAL /usr/local/java-home

ENV JAVA_HOME=/usr/local/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
# Copy from docker/airflow directory (relative to build context which is project root)
COPY docker/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt