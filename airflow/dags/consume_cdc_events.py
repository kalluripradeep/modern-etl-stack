"""
Real-time CDC Consumer: Kafka → PostgreSQL Destination
Consumes order changes from Kafka and updates warehouse in real-time
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
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

def consume_and_process():
    """Consume CDC events from Kafka and process them"""
    
    print("🔄 Starting CDC consumer...")
    
    # Connect to Kafka
    consumer = KafkaConsumer(
        'cdc.public.orders',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cdc-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Stop after 10 seconds if no messages
    )
    
    # Connect to destination database
    conn = psycopg2.connect(
        host='postgres-dest',
        port=5432,
        database='destdb',
        user='destuser',
        password='destpass'
    )
    
    processed = 0
    max_messages = 100  # Process up to 100 messages per run
    
    print(f"📊 Consuming CDC events (max {max_messages})...")
    
    try:
        for message in consumer:
            if processed >= max_messages:
                break
                
            event = message.value
            
            # Debezium event structure
            if 'payload' in event:
                payload = event['payload']
            else:
                payload = event  # Sometimes payload is at root
            
            operation = payload.get('op')
            
            if operation:
                print(f"📦 Processing operation: {operation}")
                
                if operation == 'r' or operation == 'c':  # READ (snapshot) or CREATE (INSERT)
                    after = payload.get('after')
                    if after:
                        upsert_order(conn, after)
                        print(f"✅ Upserted order {after.get('order_id')}")
                    
                elif operation == 'u':  # UPDATE
                    after = payload.get('after')
                    if after:
                        upsert_order(conn, after)
                        print(f"✅ Updated order {after.get('order_id')}")
                    
                elif operation == 'd':  # DELETE
                    before = payload.get('before')
                    if before:
                        delete_order(conn, before.get('order_id'))
                        print(f"✅ Deleted order {before.get('order_id')}")
            
            processed += 1
            
    except Exception as e:
        print(f"⚠️ Error processing messages: {e}")
    finally:
        consumer.close()
        conn.close()
    
    print(f"✅ Processed {processed} CDC events")
    
    return processed

def upsert_order(conn, data):
    """Insert or update order in destination"""
    try:
        with conn.cursor() as cur:
            # Convert microseconds to timestamp if needed
            order_date = data.get('order_date')
            created_at = data.get('created_at')
            updated_at = data.get('updated_at')
            
            # Handle timestamp conversion
            if isinstance(order_date, int):
                order_date = f"to_timestamp({order_date}/1000000.0)"
            else:
                order_date = f"'{order_date}'"
                
            if isinstance(created_at, int):
                created_at = f"to_timestamp({created_at}/1000000.0)"
            else:
                created_at = f"'{created_at}'"
                
            if isinstance(updated_at, int):
                updated_at = f"to_timestamp({updated_at}/1000000.0)"
            else:
                updated_at = f"'{updated_at}'"
            
            cur.execute(f"""
                INSERT INTO public.orders (
                    order_id, customer_id, order_date, 
                    total_amount, status, created_at, updated_at
                ) VALUES (
                    {data['order_id']}, 
                    {data['customer_id']}, 
                    {order_date}, 
                    {data['total_amount']}, 
                    '{data['status']}', 
                    {created_at}, 
                    {updated_at}
                )
                ON CONFLICT (order_id) 
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    total_amount = EXCLUDED.total_amount,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
            """)
            conn.commit()
    except Exception as e:
        print(f"❌ Error upserting order: {e}")
        conn.rollback()

def delete_order(conn, order_id):
    """Delete order from destination"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM public.orders
                WHERE order_id = {order_id}
            """)
            conn.commit()
    except Exception as e:
        print(f"❌ Error deleting order: {e}")
        conn.rollback()

task_consume = PythonOperator(
    task_id='consume_cdc_events',
    python_callable=consume_and_process,
    dag=dag,
)