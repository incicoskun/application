from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    regexp_extract,
    split,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("NASA Log Analyzer")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "logs-raw-data")
    .option("startingOffsets", "latest")
    .load()
)

schema = StructType(
    [StructField("raw_log", StringType()), StructField("producer_time", DoubleType())]
)

json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# regexle parcalama
host_pattern = r'^(\S+) \S+ \S+ \[(.*?)\] "(.*?)" (\d{3})'

parsed_df = (
    json_df.withColumn("ip_address", regexp_extract("raw_log", host_pattern, 1))
    .withColumn("timestamp_str", regexp_extract("raw_log", host_pattern, 2))
    .withColumn("request_str", regexp_extract("raw_log", host_pattern, 3))
    .withColumn("status_code", regexp_extract("raw_log", host_pattern, 4).cast("int"))
)

final_df = (
    parsed_df.withColumn("endpoint", split(col("request_str"), " ").getItem(1))
    .withColumn("uuid", expr("uuid()"))
    .withColumn("log_time", current_timestamp())
)

clean_df = final_df.filter(col("endpoint").isNotNull() & (col("endpoint") != ""))


def write_to_cassandra(batch_df, batch_id):
    batch_df.select(
        "uuid", "ip_address", "endpoint", "status_code", "log_time"
    ).write.format("org.apache.spark.sql.cassandra").options(
        table="logs_raw", keyspace="nasa_logs"
    ).mode("append").save()


query = (
    clean_df.writeStream.foreachBatch(write_to_cassandra).outputMode("update").start()
)

query.awaitTermination()
