select event_id
from {{ source('silver', 'inventory_events') }}
where movement_type = 'RESTOCK' and quantity_change < 0
