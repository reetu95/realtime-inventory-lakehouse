# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.checkpoint;

# COMMAND ----------

from pyspark.sql import functions as F

movements = (spark.readStream.table("workspace.silver.inventory_events")
             .filter("event_type = 'inventory_movement'"))

# COMMAND ----------

stock = (
        movements
         .groupBy(
             "product_id", 
             "product_description", 
             "warehouse_id", 
             "warehouse_country")
         .agg(
             F.sum("quantity_change").alias("quantity_on_hand"),
             F.count("*").alias("movement_count"),
             F.max("event_ts").alias("last_movement_ts")
         ))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.checkpoints;

# COMMAND ----------

stock_q = (stock.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "/Volumes/workspace/gold/checkpoints/stock_levels")
    .trigger(availableNow=True)
    .toTable("workspace.gold.stock_levels"))

stock_q.awaitTermination()

# COMMAND ----------

