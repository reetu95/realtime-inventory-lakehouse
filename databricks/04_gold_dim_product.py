# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gold.seed_files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed The Dimension

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

seed = (
    spark.read.csv("/Volumes/workspace/gold/seed_files/ref_products.csv", header = True)
    .select(
        F.col("StockCode").alias("product_id"),
        F.col("description").alias("product_description"),
        F.col("unit_price").cast("double").alias("unit_price")
    )
)

(seed
    .withColumn("effective_start_ts", F.to_timestamp(F.lit("2026-01-01 00:00:00")))
    .withColumn("effective_end_ts", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
    .withColumn("is_current", F.lit(True))
    .write.mode("overwrite")
    .saveAsTable("workspace.gold.dim_product"))


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.gold.dim_product SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE workspace.gold.dim_product RENAME COLUMN prodcut_description TO product_description;

# COMMAND ----------

display(spark.sql(
    """ 
        SELECT 
            count(*) AS total_rows, 
            count_if(is_current) AS current_rows
        FROM 
            workspace.gold.dim_product
    """
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The SCD Type 2 engine

# COMMAND ----------

def scd2_upsert(batch_df, batch_id):
    """ 
    A product's price can change more than once within a single batch.
    Keep only the latest update per product;
    Applying all would create multiple "current" rows and violate the SCD invariant """
    spark = batch_df.sparkSession
    latest = (
        batch_df.withColumn("run", F.row_number().over(
            Window.partitionBy("product_id").orderBy(F.col("event_ts").desc())))
        .filter("run = 1")
        .select("product_id",
                "product_description",
                F.col("new_price").alias("unit_price"),
                "event_ts")
    )

    dim = DeltaTable.forName(spark, "workspace.gold.dim_product")

    # Step 1 - Expire : Close the current row for products whose price changes
    (dim.alias("t")
     .merge(latest.alias("s"),
            "t.product_id = s.product_id AND t.is_current = true")
     .whenMatchedUpdate(
         condition = "t.unit_price <> s.unit_price",
         set = {
             "effective_end_ts" : "s.event_ts",
             "is_current" : "false"
         }
     ).execute())
    
    #Step 2 - insert : a new current row for every product that has no current row (i.e, exactly the ones step 1 just expired)
    still_current = (spark.table("workspace.gold.dim_product")
                     .filter("is_current = true")
                     .select("product_id"))

    new_versions = (
        latest
        .join(still_current, "product_id", "left_anti")
        .withColumn("effective_start_ts", F.col("event_ts"))
        .withColumn("effective_end_ts", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
        .withColumn("is_current", F.lit(True))
        .drop("event_ts")
    )

    (new_versions.write.format("delta").mode("append")
        .saveAsTable("workspace.gold.dim_product"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

test_df = (spark.read          # note: read, not readStream
    .table("workspace.silver.inventory_events")
    .filter("event_type = 'price_update'")
    .filter("new_price IS NOT NULL"))

scd2_upsert(test_df, 0)

# COMMAND ----------

print("TABLE schema:")
spark.table("workspace.gold.dim_product").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the stream through the engine

# COMMAND ----------

price_updates = (
    spark.readStream
    .table("workspace.silver.inventory_events")
    .filter("event_type = 'price_update'")
    .filter("new_price IS NOT NULL")
)

scd_q = (
    price_updates.writeStream
    .foreachBatch(scd2_upsert)
    .option("checkpointLocation", "/Volumes/workspace/gold/checkpoints/dim_product_scd2")
    .trigger(availableNow = True)
    .start()
)

scd_q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

# A) Products with history, the SCD money shot

display(spark.sql(
    """ 
    SELECT 
        product_id, 
        product_description,
        unit_price,
        effective_start_ts,
        effective_end_ts,
        is_current
    FROM 
        workspace.gold.dim_product
    WHERE 
        product_id IN (
            SELECT 
                product_id 
            FROM 
                workspace.gold.dim_product
            GROUP BY 
                product_id
            HAVING
                count(*) > 1
            )
    ORDER BY 
        product_id,
        effective_start_ts
        LIMIT 30"""
))

# COMMAND ----------

# B) THE INVARIANT - Must return zero rows
display(
    spark.sql(
        """
        SELECT
            product_id,
            count_if(is_current) AS current_count
        FROM
            workspace.gold.dim_product
        GROUP BY
            product_id
        HAVING
            count_if(is_current) <> 1"""
    )
)

# COMMAND ----------

#C) Version stats
display(
    spark.sql(
        """
        SELECT
            count(*) AS total_rows,
            count(DISTINCT product_id) AS products,
            count_if(is_current) AS current_rows
        FROM
            workspace.gold.dim_product """
    )
)

# COMMAND ----------

