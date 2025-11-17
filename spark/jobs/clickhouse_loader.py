from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
        .getOrCreate()

def load_to_clickhouse():
    spark = create_spark_session()
    
    # Чтение данных из Iceberg
    customers_df = spark.table("spark_catalog.warehouse.customers")
    orders_df = spark.table("spark_catalog.warehouse.orders")
    
    # Трансформации для ClickHouse
    enriched_orders = orders_df \
        .join(customers_df, "id") \
        .select(
            col("orders.id").alias("order_id"),
            col("orders.customer_id"),
            col("customers.name").alias("customer_name"),
            col("orders.amount"),
            col("orders.status"),
            col("orders.created_at"),
            date_format(col("orders.created_at"), "yyyy-MM").alias("order_month")
        )
    
    # Запись в ClickHouse
    enriched_orders.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/analytics") \
        .option("dbtable", "orders_enriched") \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("overwrite") \
        .save()
    
    print("Data successfully loaded to ClickHouse!")

if __name__ == "__main__":
    load_to_clickhouse()