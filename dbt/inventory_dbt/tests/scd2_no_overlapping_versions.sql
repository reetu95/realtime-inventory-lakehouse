select a.product_id
from {{ source('gold', 'dim_product') }} a
join {{ source('gold', 'dim_product') }} b
  on a.product_id = b.product_id
 and a.effective_start_ts < b.effective_end_ts
 and b.effective_start_ts < a.effective_end_ts
 and a.effective_start_ts <> b.effective_start_ts
