FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && JAVA_BIN=$(update-alternatives --list java 2>/dev/null | head -1 \
        || find /usr/lib/jvm -maxdepth 5 -name "java" \( -type f -o -type l \) 2>/dev/null | head -1) \
    && echo "JAVA_BIN=$JAVA_BIN" \
    && test -n "$JAVA_BIN" \
    && cp "$JAVA_BIN" /usr/local/bin/java \
    && chmod +x /usr/local/bin/java \
    && /usr/local/bin/java -version

USER airflow
# Copy from docker/airflow directory (relative to build context which is project root)
COPY docker/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt