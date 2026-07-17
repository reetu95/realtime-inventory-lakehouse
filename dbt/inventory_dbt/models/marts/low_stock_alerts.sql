select
    product_id,
    product_description,
    warehouse_id,
    quantity_on_hand,
    last_movement_ts
from {{ source('gold', 'stock_levels') }}
where quantity_on_hand < 10
