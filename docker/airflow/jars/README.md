# Offline JARs for the Airflow image

This directory is normally empty. The image build downloads the Spark JARs it
needs from Maven Central, and that is the usual path.

Put files here only when the build host **cannot reach `repo1.maven.org`** —
a corporate proxy, an air-gapped network, or a firewall that blocks Maven
Central. Any file present here is copied into the image instead of being
downloaded, so the build stops needing the internet.

Partial use is fine: drop in only the ones that fail and the rest are still
fetched normally.

## Files

Download these on a machine with access, then copy them into this directory:

```
hadoop-aws-3.3.4.jar
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

aws-java-sdk-bundle-1.12.262.jar
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

iceberg-spark-runtime-3.5_2.12-1.4.2.jar
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

postgresql-42.7.4.jar
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

The names must match exactly — the build looks for these filenames.

## Versions

They are not arbitrary. `pyspark` 3.5.0 bundles Hadoop 3.3.4, so `hadoop-aws`
has to match it, and `iceberg-spark-runtime` has to match Spark 3.5 with Scala
2.12. If you bump `pyspark` in `requirements-airflow.txt`, these move with it.

The `.jar` files themselves are git-ignored — they are large binaries and do
not belong in the repository.
