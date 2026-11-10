"""Iceberg maintenance, on a schedule of its own (#225).

This used to be the last task of spark_transform_silver, gated on the hour of
`data_interval_end` so it ran once a day rather than once an hour. That gate
cost two days of diagnosis on a cluster that was filling up, for three
reasons worth recording:

  * `data_interval_end` is not the time the task runs. A run triggered by
    ingest_source_to_bronze gets the interval *containing* it, not the one
    that just closed, so a triggered run at 00:08 rendered 01:00 and skipped.
  * It is in UTC, and the Airflow UI shows local time. The run that does the
    work displays as 18:00 in US Central, so the obvious row to click -- the
    one labelled 00:00 -- is the one that skips.
  * Twenty-three runs a day logged "Skipping Iceberg maintenance" and exited
    0. Nothing distinguishes that from a day when maintenance was broken.

Here "did maintenance run today" is one DAG run with one task, in the state
it actually finished in. The job is unchanged; only when it is invoked is.

Deliberately not depending on the silver transforms. Expiring snapshots is
what releases disk, and a failed transform is no reason to stop reclaiming
it -- that was true of the old trigger_rule='all_done' and it is true here.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from spark_submit import SPARK_SECRET_ENV, get_spark_submit_command

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    # One retry only: the job is idempotent, but a second full compaction on
    # a disk that is already short of space is not obviously an improvement.
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'iceberg_maintenance',
    default_args=default_args,
    description='Expire snapshots, remove orphan files, compact (Sundays)',
    # Daily at midnight UTC, which is what the old gate was reaching for.
    schedule='@daily',
    # Never run more than one of these at a time, and never backfill a week
    # of them after an outage: each one rewrites files, and the only thing
    # that matters is that the most recent one ran.
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=['iceberg', 'maintenance', 'storage'],
) as dag:

    maintenance = BashOperator(
        task_id='iceberg_maintenance',
        bash_command=get_spark_submit_command(
            'Iceberg-Maintenance', '/opt/spark-jobs/iceberg_maintenance.py',
        ).strip(),
        env=SPARK_SECRET_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )
