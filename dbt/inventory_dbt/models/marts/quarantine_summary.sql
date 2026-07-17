select
    quarantine_reason,
    count(*) as event_count,
    min(kafka_timestamp) as first_seen,
    max(kafka_timestamp) as last_seen
from {{ source('silver', 'inventory_events_quarantine') }}
group by quarantine_reason
