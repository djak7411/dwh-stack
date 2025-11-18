from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergToClickHouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9222") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

def load_to_clickhouse():
    spark = create_spark_session()
    
    # Чтение данных из Iceberg
    customers_df = spark.read.format("iceberg").load("spark_catalog.warehouse.customers")
    orders_df = spark.read.format("iceberg").load("spark_catalog.warehouse.orders")
    
    print(f"Loaded {customers_df.count()} customers from Iceberg")
    print(f"Loaded {orders_df.count()} orders from Iceberg")
    
    # Запись в ClickHouse (пример)
    customers_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/analytics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "customers") \
        .option("user", "admin") \
        .option("password", "password") \
        .mode("append") \
        .save()
    
    orders_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/analytics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("dbtable", "orders") \
        .option("user", "admin") \
        .option("password", "password") \
        .mode("append") \
        .save()
    
    print("Data loaded to ClickHouse successfully!")
    spark.stop()

if __name__ == "__main__":
    load_to_clickhouse()