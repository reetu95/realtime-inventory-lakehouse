with counts as (
  select
    (select count(*) from workspace.bronze.inventory_events_raw) as bronze_n,
    (select count(*) from {{ source('silver', 'inventory_events') }}) as clean_n,
    (select count(*) from {{ source('silver', 'inventory_events_quarantine') }}) as quarantine_n
)
select * from counts
where bronze_n <> clean_n + quarantine_n
