FROM apache/spark:3.5.0

USER root

# Hadoop 3.3.4 is bundled with Spark 3.5.0 — add S3A connector JARs
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
