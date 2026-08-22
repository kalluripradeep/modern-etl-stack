"""
Spark SQL Orchestration: Bronze -> Silver (Elite Scalability)
Parallelized transformation tasks with NEW Weekly Iceberg Maintenance.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Fetch cluster-specific configurations from environment (set via Docker or Helm)
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')

# Iceberg JDBC catalog lives in postgres-dest (schema iceberg_catalog) so
# Trino can query the same tables Spark writes.
DEST_DB_HOST = os.getenv('DEST_DB_HOST', 'postgres-dest')
DEST_DB_PORT = os.getenv('DEST_DB_PORT', '5432')
DEST_DB_NAME = os.getenv('DEST_DB_NAME', 'destdb')
DEST_DB_USER = os.getenv('DEST_DB_USER', 'destuser')
DEST_DB_PASSWORD = os.getenv('DEST_DB_PASSWORD', 'destpass')
ICEBERG_CATALOG_URI = (
    f"jdbc:postgresql://{DEST_DB_HOST}:{DEST_DB_PORT}/{DEST_DB_NAME}"
    "?currentSchema=iceberg_catalog"
)

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
    tags=['spark', 'iceberg', 'silver', 'scalability', 'compaction'],
) as dag:

    # The maintenance gate below falls back to "now" when the run has no
    # interval. The four transform tasks used to take a date too, and no
    # longer do: the Spark jobs read every retained Bronze partition rather
    # than one day's, so a day they missed is merged on the next run instead
    # of being pruned away unmerged (see spark/jobs/bronze.py).
    #
    # In Airflow 3 a manually triggered run has logical_date=None unless one is
    # passed explicitly, and the whole family of macros derived from it --
    # ds_nodash, ts, and data_interval_end alike -- is then simply absent from
    # the template context. Rendering fails before the task starts:
    #   UndefinedError: 'ds_nodash' is undefined
    # so the DAG could not be triggered from the UI or from `airflow dags
    # trigger` at all. Only the scheduler and the TriggerDagRunOperator in
    # ingest_source_to_bronze supplied one, which is why it went unnoticed:
    # the automated paths always worked.
    #
    # The `| default(...)` filter is what makes it safe, because it tolerates
    # an undefined name rather than a null one. Today's date is the right
    # fallback anyway -- the Spark jobs already default to it when no argument
    # is passed (see transform_orders.py), so this only makes the DAG agree
    # with the jobs it calls.

    # Secrets are passed to the task's environment, never interpolated into the
    # command string. Airflow stores every bash_command it renders and shows it
    # under "Rendered Template" in the UI, so a password written into the
    # command is a password in the metadata database, in the task log, and on
    # any screen showing that page. Referencing $MINIO_ROOT_PASSWORD instead
    # keeps the literal name in all three places and lets the shell substitute
    # the value at exec time.
    #
    # This does not hide the value from `ps` inside the task container -- Spark
    # takes these as --conf arguments, so they land in the process's argv
    # either way. Closing that too means a Spark properties file mounted from a
    # secret, which is a larger change than this one.
    SPARK_SECRET_ENV = {
        'MINIO_ROOT_USER': MINIO_USER,
        'MINIO_ROOT_PASSWORD': MINIO_PASSWORD,
        'DEST_DB_USER': DEST_DB_USER,
        'DEST_DB_PASSWORD': DEST_DB_PASSWORD,
    }

    # Helper function to generate standardized spark-submit commands
    def get_spark_submit_command(job_name, script_path, additional_args=""):
        # spark.driver.host: advertise the driver's IP, not its hostname.
        #
        # These run in client mode, so the driver lives in whatever container
        # Airflow gave the task and every executor has to open a connection
        # back to it. Left unset, Spark advertises the local hostname. Under
        # docker-compose that is the container name and Docker's DNS resolves
        # it, so this works and hides the problem. Under KubernetesExecutor it
        # is the task pod's name -- and a bare pod has no Service, so nothing
        # in the cluster can resolve it. Executors then launch with
        #   --driver-url spark://CoarseGrainedScheduler@<task-pod-name>:<port>
        # fail to reach it, and die with "Command exited with code 1". The
        # master immediately grants a replacement, which dies the same way:
        # one cluster reached executor ID 45962 before anyone noticed, while
        # the driver sat logging "Initial job has not accepted any resources"
        # and the workers looked idle because they were, between deaths.
        #
        # The pod IP is routable, so pin to it. bindAddress stays 0.0.0.0 so
        # the driver still listens on all interfaces inside the container.
        #
        # --jars, not --conf spark.jars.packages: the JARs are baked into the
        # image (see docker/airflow/Dockerfile) instead of resolved from Maven
        # Central on every run. That download was ~1GB per task -- slow, the
        # thing that filled the node's ephemeral storage before #127, and a
        # hard failure whenever the cluster cannot reach repo1.maven.org:
        #   :: UNRESOLVED DEPENDENCIES ::
        #   module not found: org.apache.iceberg#iceberg-spark-runtime...
        # Only these two are listed because the Spark workers already ship
        # hadoop-aws and the AWS SDK; the driver serves these to the executors.
        return f"""
    spark-submit \
        --master {SPARK_MASTER_URL} \
        --conf spark.driver.host=$(hostname -i | cut -d' ' -f1) \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --jars /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/spark-jars/postgresql-42.7.4.jar \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
        --conf spark.sql.catalog.silver.uri={ICEBERG_CATALOG_URI} \
        --conf spark.sql.catalog.silver.jdbc.user=$DEST_DB_USER \
        --conf spark.sql.catalog.silver.jdbc.password=$DEST_DB_PASSWORD \
        --conf spark.sql.catalog.silver.warehouse=s3a://silver/ \
        --name {job_name} {script_path} {additional_args}
    """

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
    )

    transform_products = BashOperator(
        task_id='transform_products',
        bash_command=get_spark_submit_command('Products-Bronze-to-Silver', '/opt/spark-jobs/transform_products.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
    )

    transform_order_items = BashOperator(
        task_id='transform_order_items',
        bash_command=get_spark_submit_command('OrderItems-Bronze-to-Silver', '/opt/spark-jobs/transform_order_items.py'),
        env=SPARK_SECRET_ENV,
        append_env=True,
    )

    # Weekly Maintenance: Compaction & Z-Ordering (Essential for 1 Billion+ records)
    maintenance_task = BashOperator(
        task_id='iceberg_maintenance',
        bash_command=(
            # Gate on the hour only (%H == "00"), so this runs once a day
            # rather than once an hour. It used to be gated to Sunday as well,
            # but the job now expires Iceberg snapshots on every run and only
            # compacts on Sundays -- expiry is what releases disk, and waiting
            # a week to release disk is how a cluster fills up. The job decides
            # which mode it is in; this only decides how often it is invoked.
            # -u pins the comparison to UTC so it cannot drift with container TZ.
            'if [ "$(date -u -d \'{{ data_interval_end | default(macros.datetime.utcnow()) }}\' +%H)" = "00" ]; then '
            # .strip() matters: the builder's f-string ends with a newline and
            # indent, so without it the "; else" lands on its own line starting
            # with a semicolon -- a bash syntax error that exits 2 before the
            # gate is ever evaluated. The other tasks pass the string through
            # as a whole command, where the stray whitespace is harmless, so
            # only this composed one breaks.
            + get_spark_submit_command('Iceberg-Maintenance', '/opt/spark-jobs/iceberg_maintenance.py').strip()
            + '; else echo "Skipping Iceberg maintenance — runs once daily at 00:00 UTC"; fi'
        ),
        env=SPARK_SECRET_ENV,
        append_env=True,
        # Run only on Sundays to optimize storage after weekly activity
        execution_timeout=timedelta(hours=2),
    )

    # Transformation happens sequentially to prevent Maven/Ivy cache download collisions
    transform_orders >> transform_customers >> transform_products >> transform_order_items >> maintenance_task

    # Maintenance is logically downstream, but you can schedule it separately.
    # Here, we trigger it once a week, but the task exists in the same DAG for visibility.
    # In a true 1bn row system, you might trigger this on a separate weekly DAG.
