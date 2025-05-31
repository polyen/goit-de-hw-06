import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
from config import kafka_config, topic_prefix

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", f'{topic_prefix}_sensors_streaming') \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5100") \
    .option("failOnDataLoss", "false") \
    .load()

json_schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("sensor_id", StringType(), True),
])

clean_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(col("key").alias("key"), col("value").alias("value")) \
    .select(col("key"), from_json(col("value"), json_schema).alias("new_value")) \
    .select(col("key"), col("new_value.*")) \
    .withColumn("timestamp", from_unixtime(col("timestamp").cast(DoubleType()))) \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("temperature", col("temperature").cast(DoubleType())) \
    .withColumn("humidity", col("humidity").cast(DoubleType())) \
    .withColumn("sensor_id", col("sensor_id").cast(StringType())) \
    .withColumn("key", col("key").cast(StringType())) \
    .withColumn("value", struct(col("temperature"), col("humidity"))) \
    .drop("new_value")

window_df = clean_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
    window(col("timestamp"), "1 minute", "30 seconds"),
    col("sensor_id")
) \
    .agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity"),
) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("sensor_id"),
    col("avg_temperature"),
    col("avg_humidity")
) \
    .withColumn("key", concat(col("sensor_id"), lit("_"), col("window_start"))) \
    .withColumn("value", struct(
    col("avg_temperature").alias("temperature"),
    col("avg_humidity").alias("humidity"),
    col("window_start").alias("start"),
    col("window_end").alias("end")
))

# Виведення даних на консоль для моніторингу
console_query = window_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

# Читання умов алертів з CSV
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True),
])

alerts_df = spark.read.csv("alerts_conditions.csv", header=True, schema=alerts_schema)

alerts_triggered_df = window_df.crossJoin(alerts_df).filter(
    ((window_df.avg_temperature >= alerts_df.temperature_min) & (window_df.avg_temperature <= alerts_df.temperature_max) ) |
    ((window_df.avg_humidity >= alerts_df.humidity_min) & (window_df.avg_humidity <= alerts_df.humidity_max))
).select(
    concat_ws("_", col("sensor_id"), col("code"), col("window_start")).alias("key"),
    struct(
        struct(
            col("window_start").alias("start"),
            col("window_end").alias("end")
        ).alias("window"),
        col("avg_temperature").alias("t_avg"),
        col("avg_humidity").alias("h_avg"),
        col("code"),
        col("message"),
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp")
    ).alias("value")
).withColumn("value", to_json(col("value")))

kafka_df = alerts_triggered_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

kafka_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

# Запис алертів у Kafka
query = kafka_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", f"{topic_prefix}_spark_streaming_out") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-3") \
    .start() \
    .awaitTermination()
