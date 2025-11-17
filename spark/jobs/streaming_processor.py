from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToIcebergStreaming") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def create_iceberg_tables(spark):
    """Создание Iceberg таблиц если они не существуют"""
    
    # Таблица customers
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.warehouse.customers (
            id INT,
            name STRING,
            email STRING,
            created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)
    
    # Таблица orders
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.warehouse.orders (
            id INT,
            customer_id INT,
            amount DECIMAL(10,2),
            status STRING,
            created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
    """)

def process_kafka_stream():
    spark = create_spark_session()
    create_iceberg_tables(spark)
    
    # Схема для Debezium JSON
    schema = StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("created_at", TimestampType())
        ])),
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("created_at", TimestampType())
        ])),
        StructField("source", StructType([
            StructField("table", StringType())
        ])),
        StructField("op", StringType())
    ])
    
    # Чтение из Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "postgres.public.customers,postgres.public.orders") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Парсинг JSON
    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp")
    ).select(
        col("data.after.*"),
        col("data.source.table"),
        col("data.op"),
        col("timestamp").alias("processed_at")
    ).filter(col("op").isin(["c", "u", "r"]))  # create, update, read
    
    def write_to_iceberg(batch_df, batch_id):
        if batch_df.count() > 0:
            # Customers
            customers_df = batch_df.filter(col("table") == "customers")
            if customers_df.count() > 0:
                customers_df.select("id", "name", "email", "created_at") \
                    .write \
                    .format("iceberg") \
                    .mode("append") \
                    .save("spark_catalog.warehouse.customers")
                print(f"Written {customers_df.count()} customers to Iceberg")
            
            # Orders
            orders_df = batch_df.filter(col("table") == "orders")
            if orders_df.count() > 0:
                orders_df.select("id", "customer_id", "amount", "status", "created_at") \
                    .write \
                    .format("iceberg") \
                    .mode("append") \
                    .save("spark_catalog.warehouse.orders")
                print(f"Written {orders_df.count()} orders to Iceberg")

    # Запись в Iceberg
    query = parsed_df.writeStream \
        .foreachBatch(write_to_iceberg) \
        .option("checkpointLocation", "s3a://checkpoints/streaming/") \
        .outputMode("update") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_kafka_stream()