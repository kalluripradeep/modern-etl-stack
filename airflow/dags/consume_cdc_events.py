"""
Real-time CDC Consumer: Kafka → PostgreSQL Destination
Consumes order changes from Kafka and updates warehouse in real-time
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer
import json
import os
import psycopg2

default_args = {
    'owner': 'pradeep',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'consume_cdc_events',
    default_args=default_args,
    description='Consume CDC events from Kafka and update warehouse',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['cdc', 'real-time', 'kafka'],
)


def _convert_timestamp(value):
    """Convert Debezium microsecond integer timestamps to Python datetime."""
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1_000_000.0, tz=timezone.utc)
    return value


def consume_and_process():
    """Consume CDC events from Kafka and process them"""

    print("Starting CDC consumer...")

    consumer = KafkaConsumer(
        'cdc.public.orders',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cdc-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000,
    )

    conn = psycopg2.connect(
        host=os.environ.get('DEST_DB_HOST', 'postgres-dest'),
        port=int(os.environ.get('DEST_DB_PORT', 5432)),
        database=os.environ.get('DEST_DB_NAME', 'destdb'),
        user=os.environ.get('DEST_DB_USER', 'destuser'),
        password=os.environ.get('DEST_DB_PASSWORD', 'destpass'),
    )

    # FIX 1: configurable via env — default small, override large via CDC_MAX_MESSAGES
    max_messages = int(os.environ.get('CDC_MAX_MESSAGES', 100))
    # FIX 2: batch commit every N rows — override via CDC_COMMIT_EVERY
    commit_every = int(os.environ.get('CDC_COMMIT_EVERY', 100))

    processed = 0
    print(f"Consuming CDC events (max {max_messages}, commit every {commit_every})...")

    try:
        for message in consumer:
            if processed >= max_messages:
                break

            event = message.value

            if 'payload' in event:
                payload = event['payload']
            else:
                payload = event

            operation = payload.get('op')

            if operation:
                if operation in ('r', 'c'):  # READ (snapshot) or CREATE
                    after = payload.get('after')
                    if after:
                        upsert_order(conn, after)
                        print(f"Upserted order {after.get('order_id')}")

                elif operation == 'u':  # UPDATE
                    after = payload.get('after')
                    if after:
                        upsert_order(conn, after)
                        print(f"Updated order {after.get('order_id')}")

                elif operation == 'd':  # DELETE
                    before = payload.get('before')
                    if before:
                        delete_order(conn, before.get('order_id'))
                        print(f"Deleted order {before.get('order_id')}")

            processed += 1

            # FIX 3: commit in batches, not per row
            if processed % commit_every == 0:
                conn.commit()
                print(f"Committed {processed} events")

        # final commit for remaining rows
        conn.commit()

    except Exception as e:
        print(f"Error processing messages: {e}")
        conn.rollback()
    finally:
        consumer.close()
        conn.close()

    print(f"Processed {processed} CDC events")
    return processed


def upsert_order(conn, data):
    """Insert or update order in destination — no commit here, caller commits in batch"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
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
                data['order_id'],
                data['customer_id'],
                _convert_timestamp(data.get('order_date')),
                data['total_amount'],
                data['status'],
                _convert_timestamp(data.get('created_at')),
                _convert_timestamp(data.get('updated_at')),
            ))
    except Exception as e:
        print(f"Error upserting order: {e}")
        conn.rollback()


def delete_order(conn, order_id):
    """Delete order from destination — no commit here, caller commits in batch"""
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.orders WHERE order_id = %s", (order_id,))
    except Exception as e:
        print(f"Error deleting order: {e}")
        conn.rollback()


task_consume = PythonOperator(
    task_id='consume_cdc_events',
    python_callable=consume_and_process,
    dag=dag,
)
