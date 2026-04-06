"""
CDC Consumer: Kafka -> PostgreSQL
Consumes upstream change events from Debezium and applies them to the data warehouse sink.
Orchestrated to run on a micro-batch schedule.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from kafka import KafkaConsumer

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'consume_cdc_events',
    default_args=default_args,
    description='Micro-batch consumer to sink CDC events into data warehouse',
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['cdc', 'streaming', 'sink'],
)


def _convert_timestamp(value):
    """Safely convert microseconds since epoch to UTC datetime."""
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1_000_000.0, tz=timezone.utc)
    return value


def sync_cdc_batch():
    """
    Connects to the Kafka broker, processes a batch of raw Debezium events,
    and applies corresponding UPSERTs and DELETEs to the destination database.
    Commits state back to the database on a configured batch size.
    """
    max_messages = int(os.environ.get('CDC_MAX_MESSAGES', 5000))
    commit_batch_size = int(os.environ.get('CDC_COMMIT_EVERY', 500))

    try:
        consumer = KafkaConsumer(
            'cdc.public.orders',
            bootstrap_servers=[os.environ.get('KAFKA_BROKER', 'kafka:9092')],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='cdc-orders-sink-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=5000,
        )
    except Exception as e:
        log.error(f"Failed to connect to Kafka broker: {e}")
        raise

    dest_conn = psycopg2.connect(
        host=os.environ.get('DEST_DB_HOST', 'postgres-dest'),
        port=int(os.environ.get('DEST_DB_PORT', 5432)),
        database=os.environ.get('DEST_DB_NAME', 'destdb'),
        user=os.environ.get('DEST_DB_USER', 'destuser'),
        password=os.environ.get('DEST_DB_PASSWORD', 'destpass'),
    )

    processed_count = 0
    upserts = 0
    deletes = 0

    log.info(f"Initialized CDC sync (Batch Limit: {max_messages}, Commit Rate: {commit_batch_size})")

    try:
        with dest_conn.cursor() as cursor:
            for message in consumer:
                if message.value is None:
                    continue

                if processed_count >= max_messages:
                    log.info("Reached maximum configured message boundary. Halting read loop.")
                    break

                payload = message.value.get('payload', message.value)
                operation = payload.get('op')

                if operation in ('r', 'c', 'u'):
                    record = payload.get('after')
                    if record:
                        _apply_upsert(cursor, record)
                        upserts += 1
                elif operation == 'd':
                    record = payload.get('before')
                    if record:
                        _apply_delete(cursor, record)
                        deletes += 1

                processed_count += 1

                if processed_count % commit_batch_size == 0:
                    dest_conn.commit()
                    log.info(f"Transaction committed: Processed {processed_count} events.")

            dest_conn.commit()
            
    except Exception as e:
        log.error(f"Error encountered during CDC sync: {e}")
        dest_conn.rollback()
        raise
    finally:
        consumer.close()
        dest_conn.close()

    log.info(f"CDC Sync complete. Total processed: {processed_count} (Upserts: {upserts}, Deletes: {deletes})")
    return processed_count


def _apply_upsert(cursor, record):
    """Executes atomic UPSERT against destination store."""
    cursor.execute("""
        INSERT INTO public.orders (
            order_id, customer_id, order_date,
            total_amount, status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id  = EXCLUDED.customer_id,
            order_date   = EXCLUDED.order_date,
            total_amount = EXCLUDED.total_amount,
            status       = EXCLUDED.status,
            updated_at   = EXCLUDED.updated_at
    """, (
        record['order_id'],
        record['customer_id'],
        _convert_timestamp(record.get('order_date')),
        record['total_amount'],
        record['status'],
        _convert_timestamp(record.get('created_at')),
        _convert_timestamp(record.get('updated_at')),
    ))


def _apply_delete(cursor, record):
    """Executes atomic DELETE against destination store."""
    cursor.execute("DELETE FROM public.orders WHERE order_id = %s", (record['order_id'],))


task_consume = PythonOperator(
    task_id='sync_cdc_batch',
    python_callable=sync_cdc_batch,
    dag=dag,
)
