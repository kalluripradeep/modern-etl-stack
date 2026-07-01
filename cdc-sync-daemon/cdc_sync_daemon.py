#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("cdc-sync-daemon")

# --- Configuration ---
SOURCE_DB_CONFIG = {
    'host': os.environ.get('SOURCE_DB_HOST', 'postgres-source'),
    'port': int(os.environ.get('SOURCE_DB_PORT', 5432)),
    'database': os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
    'user': os.environ.get('SOURCE_DB_USER', 'sourceuser'),
    'password': os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
}

DEST_DB_CONFIG = {
    'host': os.environ.get('DEST_DB_HOST', 'postgres-dest'),
    'port': int(os.environ.get('DEST_DB_PORT', 5432)),
    'database': os.environ.get('DEST_DB_NAME', 'destdb'),
    'user': os.environ.get('DEST_DB_USER', 'destuser'),
    'password': os.environ.get('DEST_DB_PASSWORD', 'destpass'),
}

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'cdc-sync-daemon-group')

# List of topics we are interested in
CDC_TOPICS = [
    'cdc.public.customers',
    'cdc.public.products',
    'cdc.public.orders',
    'cdc.public.order_items'
]

# Schema cache: maps table_name -> {'columns': [...], 'pk': 'column_name'}
schema_cache = {}

def get_db_connection(config):
    """Establish connection to PostgreSQL database with retries."""
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(**config)
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection failed: {e}. Retrying in 5s...")
            time.sleep(5)
            retries -= 1
    raise RuntimeError("Failed to connect to the database after several retries.")

def fetch_table_metadata(source_conn, table_name):
    """Retrieve columns, primary key, and data types from source database."""
    metadata = {'columns': [], 'pk': None, 'types': {}}
    
    with source_conn.cursor() as cur:
        # Get column names and types
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s
        """, (table_name,))
        
        columns_info = cur.fetchall()
        if not columns_info:
            logger.error(f"Table {table_name} not found in information_schema on source.")
            return None
            
        for col_name, data_type in columns_info:
            metadata['columns'].append(col_name)
            metadata['types'][col_name] = data_type
            
        # Get primary key column
        cur.execute("""
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tc 
            JOIN information_schema.key_column_usage kcu 
              ON tc.constraint_name = kcu.constraint_name 
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
              AND tc.table_schema = 'public' 
              AND tc.table_name = %s
        """, (table_name,))
        
        pk_info = cur.fetchone()
        if pk_info:
            metadata['pk'] = pk_info[0]
        else:
            # Fallback to id or first column if no PK constraint
            metadata['pk'] = metadata['columns'][0]
            logger.warning(f"No explicit PK found for table {table_name}. Defaulting to first column: {metadata['pk']}")
            
    return metadata

def ensure_destination_table(source_conn, dest_conn, table_name, metadata):
    """Create the destination table in postgres-dest public schema if it doesn't exist."""
    with dest_conn.cursor() as cur:
        # Check if table exists in dest
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            )
        """, (table_name,))
        
        exists = cur.fetchone()[0]
        if exists:
            return
            
        logger.info(f"Table public.{table_name} does not exist in destination. Re-creating structure...")
        
        # Build dynamic CREATE TABLE DDL
        with source_conn.cursor() as s_cur:
            # Instead of pg_dump, let's query the specific columns and reconstruct the DDL
            s_cur.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = s_cur.fetchall()
            
            ddl_parts = []
            for col_name, data_type, max_len, is_nullable in columns:
                type_str = data_type
                if max_len:
                    type_str += f"({max_len})"
                
                null_str = "NULL" if is_nullable == "YES" else "NOT NULL"
                
                # Check if it is the primary key
                if col_name == metadata['pk']:
                    ddl_parts.append(f'"{col_name}" {type_str} PRIMARY KEY')
                else:
                    ddl_parts.append(f'"{col_name}" {type_str} {null_str}')
                    
            create_ddl = f"CREATE TABLE public.{table_name} (\n  " + ",\n  ".join(ddl_parts) + "\n);"
            logger.info(f"Executing DDL on dest:\n{create_ddl}")
            cur.execute(create_ddl)
            dest_conn.commit()
            logger.info(f"Successfully created table public.{table_name} in destination database.")

def format_value(val, col_type):
    """Convert Debezium timestamps/numbers to Python types compatible with psycopg2."""
    if val is None:
        return None
        
    # Debezium represents timestamps as epoch microseconds or milliseconds (integers)
    if "timestamp" in col_type.lower() and isinstance(val, int):
        # Microseconds vs Milliseconds heuristic
        if val > 1e12: # Milliseconds or Microseconds
            if val > 1e15: # Microseconds
                return datetime.utcfromtimestamp(val / 1_000_000.0)
            else: # Milliseconds
                return datetime.utcfromtimestamp(val / 1000.0)
        return datetime.utcfromtimestamp(val)
        
    return val

def process_cdc_event(dest_conn, table_name, event_payload, metadata):
    """Apply CDC insert/update/delete to the destination database."""
    op = event_payload.get('op')
    before = event_payload.get('before')
    after = event_payload.get('after')
    
    pk_col = metadata['pk']
    cols = metadata['columns']
    types = metadata['types']
    
    with dest_conn.cursor() as cur:
        if op in ('c', 'r', 'u'): # Create, Read (Snapshot), Update
            if not after:
                logger.warning(f"CDC event has operation '{op}' but no 'after' payload.")
                return
                
            # Filter after values to include only columns we know exist
            record = {}
            for col_name in cols:
                if col_name in after:
                    record[col_name] = format_value(after[col_name], types[col_name])
            
            # Construct PostgreSQL Upsert
            columns_sqled = [sql.Identifier(c) for c in record.keys()]
            values_sqled = [sql.Placeholder(c) for c in record.keys()]
            
            # Build DO UPDATE SET part
            update_parts = []
            for col_name in record.keys():
                if col_name != pk_col:
                    update_parts.append(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(col_name),
                            sql.Identifier(col_name)
                        )
                    )
            
            if update_parts:
                upsert_query = sql.SQL("""
                    INSERT INTO public.{} ({})
                    VALUES ({})
                    ON CONFLICT ({}) DO UPDATE SET {}
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(columns_sqled),
                    sql.SQL(', ').join(values_sqled),
                    sql.Identifier(pk_col),
                    sql.SQL(', ').join(update_parts)
                )
            else:
                # Primary key only table
                upsert_query = sql.SQL("""
                    INSERT INTO public.{} ({})
                    VALUES ({})
                    ON CONFLICT ({}) DO NOTHING
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(columns_sqled),
                    sql.SQL(', ').join(values_sqled),
                    sql.Identifier(pk_col)
                )
                
            cur.execute(upsert_query, record)
            dest_conn.commit()
            logger.debug(f"Applied UPSERT on public.{table_name} for PK {record[pk_col]}")
            
        elif op == 'd': # Delete
            if not before:
                logger.warning(f"CDC event has operation 'd' but no 'before' payload.")
                return
                
            pk_val = before.get(pk_col)
            if pk_val is None:
                logger.warning(f"Could not extract primary key value for delete on {table_name}.")
                return
                
            delete_query = sql.SQL("DELETE FROM public.{} WHERE {} = %s").format(
                sql.Identifier(table_name),
                sql.Identifier(pk_col)
            )
            cur.execute(delete_query, (pk_val,))
            dest_conn.commit()
            logger.debug(f"Applied DELETE on public.{table_name} for PK {pk_val}")
            
        else:
            logger.warning(f"Unknown operation op={op} on table {table_name}. Skipping.")

def main():
    logger.info("Starting CDC Sync Daemon...")
    
    # Establish database connections
    source_conn = get_db_connection(SOURCE_DB_CONFIG)
    dest_conn = get_db_connection(DEST_DB_CONFIG)
    
    # Pre-populate schemas for core tables
    logger.info("Initializing schema cache...")
    for topic in CDC_TOPICS:
        table_name = topic.split('.')[-1]
        metadata = fetch_table_metadata(source_conn, table_name)
        if metadata:
            schema_cache[table_name] = metadata
            ensure_destination_table(source_conn, dest_conn, table_name, metadata)
            logger.info(f"Cached schema for table: public.{table_name}")
            
    # Connect to Kafka
    logger.info(f"Connecting to Kafka brokers at: {KAFKA_BOOTSTRAP}")
    consumer = None
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            break
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka: {e}. Retrying in 5s...")
            time.sleep(5)
            retries -= 1
            
    if not consumer:
        logger.error("Could not connect to Kafka. Exiting.")
        sys.exit(1)
        
    logger.info(f"Subscribing to CDC topics: {CDC_TOPICS}")
    consumer.subscribe(CDC_TOPICS)
    
    logger.info("CDC Sync Daemon is listening for events...")
    
    try:
        for msg in consumer:
            # Topic format: cdc.public.<table_name>
            topic_parts = msg.topic.split('.')
            if len(topic_parts) < 3:
                continue
                
            table_name = topic_parts[-1]
            event = msg.value
            
            if not event:
                continue
                
            payload = event.get('payload', event)
            if not payload:
                continue
                
            # Dynamic schema refresh if table schema isn't cached yet
            if table_name not in schema_cache:
                logger.info(f"New table seen: {table_name}. Fetching metadata...")
                metadata = fetch_table_metadata(source_conn, table_name)
                if metadata:
                    schema_cache[table_name] = metadata
                    ensure_destination_table(source_conn, dest_conn, table_name, metadata)
                else:
                    logger.error(f"Cannot process event for table {table_name} - schema not found.")
                    continue
                    
            metadata = schema_cache[table_name]
            
            try:
                process_cdc_event(dest_conn, table_name, payload, metadata)
            except psycopg2.InterfaceError:
                logger.warning("Database connection closed. Reconnecting...")
                dest_conn = get_db_connection(DEST_DB_CONFIG)
                source_conn = get_db_connection(SOURCE_DB_CONFIG)
                process_cdc_event(dest_conn, table_name, payload, metadata)
            except Exception as e:
                logger.error(f"Error processing event for table {table_name}: {e}")
                dest_conn.rollback()
                
    except KeyboardInterrupt:
        logger.info("Stopping daemon...")
    finally:
        if consumer:
            consumer.close()
        source_conn.close()
        dest_conn.close()
        logger.info("CDC Sync Daemon stopped.")

if __name__ == '__main__':
    main()
