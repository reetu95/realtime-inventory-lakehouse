# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.silver.checkpoints")

# COMMAND ----------

bronze = (spark.readStream
          .table("workspace.bronze.inventory_events_raw")
          .withColumn("event_ts", F.to_timestamp("event_timestamp"))
          .withColumn("late_by_minutes",
                      F.round((F.unix_timestamp("kafka_timestamp") - 
                               F.unix_timestamp(F.to_timestamp("event_timestamp"))) / 60, 1))
          .withColumn("IS_LATE", F.col("late_by_minutes") > 30))

# COMMAND ----------

is_invalid = (
    F.col("event_id").isNull() | 
    F.col("product_id").isNull() |
    ((F.col("movement_type") == "RESTOCK") & (F.col("quantity_change") < 0)) |
    ((F.col("event_type") == "inventory_movement") & F.col("warehouse_id").isNull()) | 
    ((F.col("event_type") == "price_update") & (F.col("new_price").isNull() | (F.col("new_price") <= 0)))
)

validated = bronze.withColumn("is_invalid", F.coalesce(is_invalid, F.lit(False)))

# COMMAND ----------

quarantine_q = (validated
                .filter("is_invalid = true")
                .withColumn("quarantine_reason",
                            F.when(F.col("product_id").isNull(), "missing_product_id")
                            .when((F.col("movement_type") == "RESTOCK") & (F.col("quantity_change") < 0), "negative_restock")
                            .when((F.col("event_type") == "inventory_movement") & F.col("warehouse_id").isNull(), "missing_warehouse")
                            .when((F.col("event_type") == "price_update") & (F.col("new_price").isNull() | (F.col("new_price") <= 0)), "invalid_price")
                            .otherwise("other"))
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "/Volumes/workspace/silver/checkpoints/quarantine")
                .trigger(availableNow = True)
                .toTable("workspace.silver.inventory_events_quarantine"))

quarantine_q.awaitTermination()

# COMMAND ----------

clean_q = (validated
           .filter("is_invalid = false")
           .withWatermark("event_ts", "3 hours")
           .dropDuplicatesWithinWatermark(["event_id"])
           .drop("is_invalid", "raw_json")
           .writeStream
           .format("delta")
           .outputMode("append")
           .option("checkpointLocation", "/Volumes/workspace/silver/checkpoints/inventory_events")
           .trigger(availableNow = True)
           .toTable("workspace.silver.inventory_events"))

clean_q.awaitTermination()

# COMMAND ----------

display(spark.sql("""
                  SELECT 'clean' AS tbl, count(*) AS n FROM workspace.silver.inventory_events
                  UNION ALL
                  SELECT 'quarantine', count(*) FROM workspace.silver.inventory_events_quarantine"""))
                  

# COMMAND ----------

display(spark.sql("""
                  SELECT quarantine_reason, count(*) AS n
                  FROM workspace.silver.inventory_events_quarantine
                  GROUP BY quarantine_reason ORDER BY n DESC"""))

# COMMAND ----------

display(spark.sql("""
                  SELECT count(*) AS late_events_kept
                  FROM workspace.silver.inventory_events_quarantine WHERE is_late = true"""))

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.silver.inventory_events")
# spark.sql("DROP TABLE IF EXISTS workspace.silver.inventory_events_quarantine")
# dbutils.fs.rm("/Volumes/workspace/silver/checkpoints/inventory_events", True)
# dbutils.fs.rm("/Volumes/workspace/silver/checkpoints/quarantine", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(*) AS late_events_kept
# MAGIC FROM
# MAGIC     workspace.silver.inventory_events
# MAGIC WHERE
# MAGIC     IS_LATE = true;

# COMMAND ----------

