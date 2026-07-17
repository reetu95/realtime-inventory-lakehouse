select product_id, count_if(is_current) as current_count
from {{ source('gold', 'dim_product') }}
group by product_id
having count_if(is_current) <> 1
