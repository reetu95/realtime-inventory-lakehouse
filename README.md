# Real-Time Inventory Lakehouse
Streaming pipeline: Python producer → Kafka (Confluent Cloud) → Spark
Structured Streaming → Delta Lake (Bronze/Silver/Gold) on Databricks.
Dimensions seeded from the Online Retail II dataset; event stream simulated
against real products, including malformed and late-arriving events.

**Status: In progress — Week 1 (ingestion)**
