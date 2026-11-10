"""
Spark SQL Orchestration: Bronze -> Silver (Elite Scalability)

Parallelized transformation tasks. Iceberg maintenance used to be the last
task here; it now has its own daily DAG (iceberg_maintenance_dag.py, #225).
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# The Spark configuration lives in spark_submit.py because the maintenance
# DAG runs against the same Iceberg catalogue and must not drift from it.
from spark_submit import SPARK_SECRET_ENV, get_spark_submit_command  # noqa: E402

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_transform_silver',
    default_args=default_args,
    description='Spark Batch Processing - Bronze to Silver (Iceberg)',
    # Also triggered by ingest_source_to_bronze on completion; this schedule
    # is the independent fallback. Override with SILVER_SCHEDULE.
    schedule=os.environ.get('SILVER_SCHEDULE', '@hourly'),
    catchup=False,
    # Spark jobs are long-running; overlapping runs would MERGE into the same
    # Iceberg tables concurrently.
    max_active_runs=1,
    # A run that never reaches a terminal state holds the single active slot
    # for good. #144 hit exactly that: a scheduled run from 2026-08-15 sat in
    # "running" for nine days after its task hit
    #   state mismatch ... Executor reported ... finished with state failed,
    #   but the task instance's state attribute is queued
    # so every later run stayed queued behind it and the lakehouse silently
    # stopped being built. Nothing surfaced it, because a queued run is not an
    # error -- there is simply no error anywhere to find.
    #
    # dagrun_timeout makes the scheduler fail such a run instead of waiting on
    # it forever. Two hours is well clear of a real run (minutes) while still
    # clearing a zombie the same day.
    dagrun_timeout=timedelta(hours=2),
    tags=['spark', 'iceberg', 'silver', 'scalability', 'compaction'],
) as dag:

    # No task here templates a date any more. The transform tasks used to take
    # one and no longer do -- the Spark jobs read every retained Bronze
    # partition rather than one day's, so a day they missed is merged on the
    # next run instead of being pruned away unmerged (see spark/jobs/bronze.py)
    # -- and the maintenance gate that needed data_interval_end has moved to
    # the iceberg_maintenance DAG, which runs on a schedule and needs no gate.
    #
    # That matters because in Airflow 3 a manually triggered run has
    # logical_date=None unless one is passed, and every macro derived from it
    # is then absent from the template context rather than null. Rendering
    # fails before the task starts -- UndefinedError: 'ds_nodash' is undefined
    # -- so the DAG could not be triggered from the UI at all, while the
    # scheduler and TriggerDagRunOperator paths kept working and hid it.
    # Templating no dates is the cheapest way to stay out of that.


    def _ensure_silver_bucket(**_):
        """Create the Iceberg warehouse bucket if it is not there yet.

        The catalog writes to s3a://silver/, and nothing on Compose ever
        created that bucket. k8s/deploy.sh makes bronze, silver and
        airflow-logs, and deploy-smoke.yml asserts silver exists, so
        Kubernetes was covered and Compose quietly was not -- a fresh
        docker compose up ran Pipe 1 fine and died on Pipe 2 with

          Error Code: NoSuchBucket

        from deep inside a Spark stack trace, which reads like a Spark or
        Iceberg problem rather than a missing bucket.

        Ingest already creates the bronze bucket this way, so this follows
        the pattern that is there rather than adding an init container that
        only one of the two deployments would use.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id='seaweedfs_s3')
        for bucket in ('bronze', 'silver'):
            if hook.check_for_bucket(bucket):
                continue
            hook.create_bucket(bucket_name=bucket)
            print(f"Created missing bucket: {bucket}")

    ensure_buckets = PythonOperator(
        task_id='ensure_buckets',
        python_callable=_ensure_silver_bucket,
    )

    transform_orders = BashOperator(
        task_id='transform_orders',
        bash_command=get_spark_submit_command('Orders-Bronze-to-Silver', '/opt/spark-jobs/transform_orders.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
    )

    transform_customers = BashOperator(
        task_id='transform_customers',
        bash_command=get_spark_submit_command('Customers-Bronze-to-Silver', '/opt/spark-jobs/transform_customers.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
        trigger_rule='all_done',
    )

    transform_products = BashOperator(
        task_id='transform_products',
        bash_command=get_spark_submit_command('Products-Bronze-to-Silver', '/opt/spark-jobs/transform_products.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
        trigger_rule='all_done',
    )

    transform_order_items = BashOperator(
        task_id='transform_order_items',
        bash_command=get_spark_submit_command('OrderItems-Bronze-to-Silver', '/opt/spark-jobs/transform_order_items.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
        trigger_rule='all_done',
    )


    # Still sequential, but no longer all-or-nothing.
    #
    # The chain's original reason -- Maven/Ivy cache collisions between
    # concurrent spark-submits -- went away when #133 baked the JARs into the
    # image. It stays sequential for a different reason: a standalone Spark
    # cluster gives an application every free core it can, and the workers here
    # hold 2 cores and 2GB each, so four concurrent applications would leave
    # some sitting in "Initial job has not accepted any resources" -- the same
    # symptom #126 spent days on. One at a time is the safe shape.
    #
    # trigger_rule='all_done' is the part that changed. The default,
    # all_success, meant a failure in the first task marked the other three
    # upstream_failed, so one table's problem hid the state of every other
    # table. #151 made that worse rather than better: it turned Bronze-missing
    # from a silent skip into a real failure, which is correct in itself, but
    # under all_success it would have taken three healthy tables down with the
    # one broken one. The same reasoning is why iceberg_maintenance is now a
    # DAG of its own rather than the last link here: expiring snapshots is what
    # releases disk, and a failed transform is no reason to stop reclaiming it.
    def _surface_transform_failure(**_):
        """Fail the run when any transform failed.

        trigger_rule='all_done' on the downstream tasks stops one table's
        failure blocking the other three, which is what it is there for. It
        also means the last task in the chain succeeds regardless, and the DAG
        run takes its state from that -- so #144 saw transform_orders red and
        the run green at the same time, which is the worst of both.

        This task runs only when something upstream failed (one_failed) and is
        skipped otherwise, so a clean run stays green while a broken table can
        no longer hide behind a successful maintenance step.
        """
        raise AirflowException(
            "At least one silver transform failed - see the red task above. "
            "The run is failed here because trigger_rule='all_done' lets the "
            "rest of the chain finish, which would otherwise leave the run "
            "green with a broken table."
        )

    surface_failure = PythonOperator(
        task_id='surface_transform_failure',
        python_callable=_surface_transform_failure,
        trigger_rule='one_failed',
    )

    ensure_buckets >> transform_orders >> transform_customers >> transform_products >> transform_order_items
    [transform_orders, transform_customers, transform_products,
     transform_order_items] >> surface_failure

    # Iceberg maintenance is no longer part of this chain. It is its own daily
    # DAG -- see airflow/dags/iceberg_maintenance_dag.py and #225.
