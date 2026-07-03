import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

# 1. สร้าง Spark Session และโหลด Library ที่ต้องใช้ (Kafka, Delta, MinIO/AWS S3)
print("🚀 Starting Spark Session (This will download required jars for the first time)...")
spark = SparkSession.builder \
    .appName("CryptoLakehouseProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. อ่านข้อมูลแบบ Streaming จาก Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto_trades") \
    .option("startingOffsets", "latest") \
    .load()

# 4. กำหนดหน้าตาของข้อมูล (Schema) ตามที่เราส่งมาจาก Python
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True)
])

# 5. Transform ข้อมูล (แปลง Byte เป็น String -> JSON -> DataFrame แบบมีคอลัมน์)
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# เพิ่มคอลัมน์เวลาให้เป็น TimeStamp 
df_final = df_parsed.withColumn("trade_timestamp", (col("trade_time") / 1000).cast("timestamp"))

print("📡 Streaming data from Kafka to MinIO (Delta Lake format)...")

# 6. เขียนข้อมูลลง MinIO ในรูปแบบ Delta Lake (Data Lakehouse)
query = df_final \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/crypto_trades") \
    .start("s3a://lakehouse/data/crypto_trades")

query.awaitTermination()
