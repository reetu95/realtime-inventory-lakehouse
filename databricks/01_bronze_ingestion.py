# Databricks notebook source
# Cell 1 — imports & config
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_BOOTSTRAP = ""   
KAFKA_KEY = "<your-confluent-kafka-api-key>"
KAFKA_SECRET = "<your-confluent-kafka-api-secret>"
KAFKA_TOPIC = "inventory-events"

# COMMAND ----------

# Cell 2 — schema
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("product_description", StringType()),
    StructField("warehouse_id", StringType()),
    StructField("warehouse_country", StringType()),
    StructField("movement_type", StringType()),
    StructField("quantity_change", IntegerType()),
    StructField("old_price", DoubleType()),
    StructField("new_price", DoubleType()),
    StructField("event_timestamp", StringType()),
])


# COMMAND ----------

# Cell 3 — read Kafka
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_KEY}" password="{KAFKA_SECRET}";')
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load())

# COMMAND ----------

# Cell 4 — parse
parsed = (raw_stream
    .select(
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition"),
        F.col("offset"))
    .withColumn("data", F.from_json("raw_json", schema))
    .select("data.*", "raw_json", "kafka_timestamp", "partition", "offset")
    .withColumn("ingestion_timestamp", F.current_timestamp()))

# COMMAND ----------

# Cell 5 — write Bronze (incremental batch)
bronze_query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/bronze/checkpoints/inventory_events_raw")
    .trigger(availableNow=True)
    .toTable("workspace.bronze.inventory_events_raw"))
bronze_query.awaitTermination()

# COMMAND ----------

# Cell 6 — validation
display(spark.sql("""
SELECT event_type, COUNT(*) AS n
FROM workspace.bronze.inventory_events_raw GROUP BY event_type ORDER BY n DESC"""))

display(spark.sql("""
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT event_id) AS distinct_event_ids,
       COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_count
FROM workspace.bronze.inventory_events_raw"""))

# COMMAND ----------

