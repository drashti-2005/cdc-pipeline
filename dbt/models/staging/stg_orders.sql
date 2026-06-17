-- ============================================================
-- Silver Layer: stg_orders
-- ============================================================
-- WHAT: Cleaned orders with status categorization
-- WHY: Consistent naming, status groupings, time calculations
-- FROM: CDC-replicated orders table (Bronze)
-- ============================================================

with source as (
    select * from {{ source('cdc', 'orders') }}
),

cleaned as (
    select
        -- Primary key
        id as order_id,
        
        -- Foreign key
        customer_id,
        
        -- Order status (standardize to lowercase)
        lower(status) as status,
        case lower(status)
            when 'pending' then 'open'
            when 'confirmed' then 'open'
            when 'shipped' then 'in_transit'
            when 'delivered' then 'completed'
            when 'cancelled' then 'cancelled'
            else 'unknown'
        end as status_category,
        
        -- Is the order still active?
        lower(status) in ('pending', 'confirmed', 'shipped') as is_active,
        lower(status) = 'delivered' as is_completed,
        lower(status) = 'cancelled' as is_cancelled,
        
        -- Financials
        total_amount,
        case
            when total_amount < 50 then 'small'
            when total_amount < 200 then 'medium'
            when total_amount < 500 then 'large'
            else 'enterprise'
        end as order_size,
        
        -- Shipping
        coalesce(trim(shipping_address), 'Not provided') as shipping_address,
        shipping_address is not null and shipping_address != '' as has_shipping_address,
        
        -- Time dimensions (for analytics)
        created_at as ordered_at,
        updated_at,
        date(created_at) as order_date,
        extract(year from created_at) as order_year,
        extract(month from created_at) as order_month,
        extract(dow from created_at) as order_day_of_week,
        extract(hour from created_at) as order_hour,
        
        -- Processing time (for completed orders)
        case 
            when lower(status) = 'delivered' 
            then extract(epoch from updated_at - created_at) / 3600.0
            else null
        end as fulfillment_hours,
        
        -- Data quality
        case
            when customer_id is null then false
            when total_amount is null or total_amount < 0 then false
            else true
        end as is_valid_record

    from source
    where id is not null
)

select * from cleaned
