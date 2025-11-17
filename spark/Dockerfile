FROM bitnami/spark:3.4.1

USER root

# Установка Python зависимостей
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    pandas==2.0.3 \
    pyiceberg==0.4.0 \
    minio==7.1.15 \
    kafka-python==2.0.2 \
    clickhouse-driver==0.2.6 \
    requests==2.31.0

# Установка JAR файлов
RUN curl -L -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.0/iceberg-spark-runtime-3.4_2.12-1.3.0.jar

RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar

RUN curl -L -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.4.6.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar

# Создание директории для jobs
RUN mkdir -p /opt/spark/jobs /opt/spark/conf
COPY jobs/ /opt/spark/jobs/
COPY conf/ /opt/spark/conf/

RUN chown -R 1001:1001 /opt/spark
USER 1001