-- ============================================================
-- Silver Layer: stg_order_items
-- ============================================================
-- WHAT: Cleaned line items with calculated line totals
-- WHY: Pre-calculate metrics used in Gold layer aggregations
-- FROM: CDC-replicated order_items table (Bronze)
-- ============================================================

with source as (
    select * from {{ source('cdc', 'order_items') }}
),

cleaned as (
    select
        -- Primary key
        id as order_item_id,
        
        -- Foreign keys
        order_id,
        product_id,
        
        -- Quantity and pricing
        quantity,
        unit_price,
        
        -- Calculated fields
        quantity * unit_price as line_total,
        
        -- Quantity buckets
        case
            when quantity = 1 then 'single'
            when quantity <= 3 then 'small_batch'
            when quantity <= 10 then 'medium_batch'
            else 'bulk'
        end as quantity_bucket,
        
        -- Timestamps
        created_at,
        
        -- Data quality
        case
            when order_id is null then false
            when product_id is null then false
            when quantity is null or quantity <= 0 then false
            when unit_price is null or unit_price <= 0 then false
            else true
        end as is_valid_record

    from source
    where id is not null
)

select * from cleaned
