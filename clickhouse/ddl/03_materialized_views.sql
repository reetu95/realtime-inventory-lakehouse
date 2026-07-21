CREATE TABLE inventory.stock_by_warehouse
(
    warehouse_id     LowCardinality(String),
    product_id       LowCardinality(String),
    quantity_on_hand Int64,
    movement_count   UInt64
)
ENGINE = SummingMergeTree
ORDER BY (warehouse_id, product_id);

CREATE MATERIALIZED VIEW inventory.mv_stock_by_warehouse
TO inventory.stock_by_warehouse AS
SELECT
    warehouse_id,
    product_id,
    sum(quantity_change) AS quantity_on_hand,
    count()              AS movement_count
FROM inventory.fact_inventory_movement
GROUP BY warehouse_id, product_id;

CREATE TABLE inventory.product_daily_trend
(
    day        Date,
    product_id LowCardinality(String),
    net_change Int64,
    units_sold Int64,
    movements  UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (product_id, day);

CREATE MATERIALIZED VIEW inventory.mv_product_daily_trend
TO inventory.product_daily_trend AS
SELECT
    toDate(event_ts)                                          AS day,
    product_id,
    sum(quantity_change)                                      AS net_change,
    sum(if(movement_type = 'SALE', -quantity_change, 0))      AS units_sold,
    count()                                                   AS movements
FROM inventory.fact_inventory_movement
GROUP BY day, product_id;

CREATE TABLE inventory.movement_cube
(
    day               Date,
    warehouse_id      LowCardinality(String),
    warehouse_country LowCardinality(String),
    movement_type     Enum8('SALE' = 1, 'RESTOCK' = 2, 'RETURN' = 3,
                            'TRANSFER' = 4, 'ADJUSTMENT' = 5),
    total_qty         AggregateFunction(sum, Int32),
    events            AggregateFunction(count),
    distinct_products AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, warehouse_id, movement_type);

CREATE MATERIALIZED VIEW inventory.mv_movement_cube
TO inventory.movement_cube AS
SELECT
    toDate(event_ts)             AS day,
    warehouse_id,
    warehouse_country,
    movement_type,
    sumState(quantity_change)    AS total_qty,
    countState()                 AS events,
    uniqState(product_id)        AS distinct_products
FROM inventory.fact_inventory_movement
GROUP BY day, warehouse_id, warehouse_country, movement_type;
