# Real-Time Inventory Lakehouse

Streaming pipeline: Python producer → Kafka (Confluent Cloud) → Spark
Structured Streaming → Delta Lake (Bronze/Silver/Gold) on Databricks.

Dimensions are seeded from the real **Online Retail II** dataset (~1M UK
e-commerce transactions); the event stream is simulated against those real
products and includes deliberately malformed and late-arriving events to
exercise the data quality layer.

## Architecture

Python event producer (simulated ERP)
        ↓
Kafka — Confluent Cloud (topic: inventory-events, 3 partitions)
        ↓
Spark Structured Streaming (Databricks)
        ↓
Bronze → Silver → Gold Delta tables
        ↓
dbt tests · Power BI

## Event types

| Event              | Share | Purpose                                  |
|--------------------|-------|------------------------------------------|
| inventory_movement | ~70%  | SALE / RESTOCK / RETURN / TRANSFER / ADJUSTMENT |
| price_update       | ~20%  | Drives SCD Type 2 history in dim_product |
| malformed / late   | ~10%  | Missing keys, invalid quantities, 2-hour-late timestamps — caught by the quality layer |

## Progress

- [x] Reference data prep: cleaned Online Retail II → 300 products, 8 warehouses (`producer/prep_reference_data.py`)
- [x] Kafka producer streaming real-product events to Confluent Cloud
- [ ] Structured Streaming ingestion → Bronze Delta (in progress)
- [ ] Silver: validation, deduplication, quarantine
- [ ] Gold: streaming stock aggregates + SCD Type 2 dim_product
- [ ] Terraform: Azure infra (ADLS, Key Vault, Databricks)
- [ ] dbt tests + freshness alerting
- [ ] CI/CD: GitHub Actions + Databricks Asset Bundles

![Events flowing in Confluent Cloud](docs/img/confluent-messages.png)

## Repo structure

producer/
  prep_reference_data.py   # cleans the Kaggle xlsx → reference CSVs
  producer.py              # streams events to Kafka
  ref_products.csv
  ref_warehouses.csv
