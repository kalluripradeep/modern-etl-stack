FROM apache/airflow:2.8.0-python3.11

# Copy from docker/airflow directory (relative to build context which is project root)
COPY docker/airflow/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt