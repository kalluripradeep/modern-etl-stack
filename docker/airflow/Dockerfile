FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && JAVA_BIN=$(find /usr/lib/jvm -maxdepth 5 -name "java" -executable -type f | head -1) \
    && echo "java found at: $JAVA_BIN" \
    && ln -sf "$JAVA_BIN" /usr/local/bin/java \
    && JAVA_HOME_DIR=$(dirname $(dirname $JAVA_BIN)) \
    && echo "export JAVA_HOME=$JAVA_HOME_DIR" > /etc/profile.d/java.sh \
    && echo "export PATH=$JAVA_HOME_DIR/bin:\$PATH" >> /etc/profile.d/java.sh \
    && java -version

USER airflow
# Copy from docker/airflow directory (relative to build context which is project root)
COPY docker/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt