"""The spark-submit invocation, in one place.

Two DAGs run Spark jobs against the same Iceberg catalogue: the silver
transforms and the maintenance that reclaims their disk (#225). Both need the
identical catalogue, S3A and JAR configuration -- a copy that drifts is a job
that writes to one catalogue and expires snapshots in another, which would
show up as "maintenance ran and freed nothing".

Nothing here touches Airflow, so it imports no DAG and defines none.
"""

import os

# Fetch cluster-specific configurations from environment (set via Docker or Helm)
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
SEAWEEDFS_ENDPOINT = os.getenv('SEAWEEDFS_ENDPOINT', 'http://seaweedfs:9000')
SEAWEEDFS_USER = os.getenv('SEAWEEDFS_ROOT_USER', 'seaweedadmin')
SEAWEEDFS_PASSWORD = os.getenv('SEAWEEDFS_ROOT_PASSWORD', 'seaweedadmin')

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

# Passed as task env rather than interpolated into the command, so the values
# are not stored in the rendered template Airflow keeps in its database.
#
# This does not hide the value from `ps` inside the task container -- Spark
# takes these as --conf arguments, so they land in the process's argv
# either way. Closing that too means a Spark properties file mounted from a
# secret, which is a larger change than this one.
SPARK_SECRET_ENV = {
    'SEAWEEDFS_ROOT_USER': SEAWEEDFS_USER,
    'SEAWEEDFS_ROOT_PASSWORD': SEAWEEDFS_PASSWORD,
    'DEST_DB_USER': DEST_DB_USER,
    'DEST_DB_PASSWORD': DEST_DB_PASSWORD,
}


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
        --conf spark.hadoop.fs.s3a.endpoint={SEAWEEDFS_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.access.key=$SEAWEEDFS_ROOT_USER \
        --conf spark.hadoop.fs.s3a.secret.key=$SEAWEEDFS_ROOT_PASSWORD \
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
