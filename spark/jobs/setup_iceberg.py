from pyspark.sql import SparkSession

def setup_iceberg_catalog():
    spark = SparkSession.builder \
        .appName("SetupIceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9222") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("=== SETTING UP ICEBERG CATALOG ===")
    
    # Создаем базу данных
    spark.sql("CREATE DATABASE IF NOT EXISTS local.analytics")
    
    # Создаем таблицу customers
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.analytics.customers (
            id INT,
            name STRING,
            email STRING,
            country_code STRING,
            created_at TIMESTAMP
        ) USING iceberg
    """)
    
    # Создаем таблицу orders
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.analytics.orders (
            id INT,
            customer_id INT,
            amount DOUBLE,
            status STRING,
            created_at TIMESTAMP
        ) USING iceberg
    """)
    
    # Вставляем тестовые данные
    spark.sql("""
        INSERT INTO local.analytics.customers VALUES
        (1, 'John Doe', 'john.doe@example.com', 'US', current_timestamp()),
        (2, 'Jane Smith', 'jane.smith@example.com', 'GB', current_timestamp()),
        (3, 'Bob Johnson', 'bob.johnson@example.com', 'CA', current_timestamp())
    """)
    
    spark.sql("""
        INSERT INTO local.analytics.orders VALUES
        (1, 1, 100.50, 'completed', current_timestamp()),
        (2, 2, 75.25, 'pending', current_timestamp()),
        (3, 1, 50.75, 'completed', current_timestamp())
    """)
    
    # Проверяем созданные таблицы
    print("=== AVAILABLE TABLES ===")
    spark.sql("SHOW TABLES IN local.analytics").show()
    
    print("=== CUSTOMERS DATA ===")
    spark.sql("SELECT * FROM local.analytics.customers").show()
    
    print("=== ORDERS DATA ===")
    spark.sql("SELECT * FROM local.analytics.orders").show()
    
    spark.stop()
    print("✅ Iceberg setup completed!")

if __name__ == "__main__":
    setup_iceberg_catalog()