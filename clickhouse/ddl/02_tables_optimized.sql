CREATE TABLE inventory.fact_inventory_movement
(
    event_id            UUID,
    event_ts            DateTime64(3, 'UTC'),
    product_id          LowCardinality(String),
    product_description LowCardinality(String),
    warehouse_id        LowCardinality(String),
    warehouse_country   LowCardinality(String),
    movement_type       Enum8('SALE' = 1, 'RESTOCK' = 2, 'RETURN' = 3,
                              'TRANSFER' = 4, 'ADJUSTMENT' = 5),
    quantity_change     Int32,
    is_late             UInt8,
    ingestion_ts        DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (warehouse_id, product_id, event_ts);

CREATE TABLE inventory.dim_product
(
    product_id          String,
    product_description String,
    unit_price          Decimal(10, 2),
    effective_start_ts  DateTime('UTC'),
    effective_end_ts    DateTime('UTC'),
    is_current          UInt8
)
ENGINE = ReplacingMergeTree
ORDER BY (product_id, effective_start_ts);

CREATE TABLE inventory.dim_warehouse
(
    warehouse_id LowCardinality(String),
    country      LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY warehouse_id;
