-- ============================================================
-- Silver Layer: stg_products
-- ============================================================
-- WHAT: Cleaned product catalog with calculated fields
-- WHY: Consistent naming, stock status flags, price tiers
-- FROM: CDC-replicated products table (Bronze)
-- ============================================================

with source as (
    select * from {{ source('cdc', 'products') }}
),

cleaned as (
    select
        -- Primary key
        id as product_id,
        
        -- Product info
        trim(name) as product_name,
        trim(category) as category,
        coalesce(trim(description), 'No description') as description,
        
        -- Pricing
        price,
        case
            when price < 25 then 'budget'
            when price < 100 then 'mid-range'
            when price < 500 then 'premium'
            else 'luxury'
        end as price_tier,
        
        -- Inventory
        stock_qty,
        case
            when stock_qty <= 0 then 'out_of_stock'
            when stock_qty < 10 then 'low_stock'
            when stock_qty < 50 then 'normal'
            else 'high_stock'
        end as stock_status,
        stock_qty <= 0 as is_out_of_stock,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Data quality
        case
            when name is null or name = '' then false
            when price is null or price <= 0 then false
            else true
        end as is_valid_record

    from source
    where id is not null
)

select * from cleaned
