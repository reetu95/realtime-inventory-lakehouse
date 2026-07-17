select event_id, movement_type
from {{ source('silver', 'inventory_events') }}
where event_type = 'inventory_movement'
  and movement_type not in ('SALE', 'RESTOCK', 'RETURN', 'TRANSFER', 'ADJUSTMENT')
