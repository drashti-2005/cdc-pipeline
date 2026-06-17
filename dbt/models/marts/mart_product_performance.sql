-- ============================================================
-- Gold Layer: Product Performance
-- ============================================================
-- WHAT: Product sales metrics and inventory analysis
-- WHY: Track bestsellers, identify slow movers, optimize stock
-- USED BY: Product managers, Inventory team, Marketing
-- ============================================================

{{ config(
    materialized='table',
    tags=['gold', 'mart', 'product']
) }}

with products as (
    select * from {{ ref('stg_products') }}
    where is_valid_record = true
),

order_items as (
    select * from {{ ref('stg_order_items') }}
    where is_valid_record = true
),

orders as (
    select * from {{ ref('stg_orders') }}
    where is_valid_record = true
),

-- Join order items with orders to get completed sales
completed_sales as (
    select
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        o.ordered_at,
        o.order_date
    from order_items oi
    join orders o on oi.order_id = o.order_id
    where o.is_completed = true  -- Only count delivered orders
),

-- Aggregate product sales metrics
product_sales as (
    select
        product_id,
        
        -- Volume metrics
        count(*) as times_ordered,
        sum(quantity) as total_units_sold,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        avg(line_total) as avg_sale_amount,
        
        -- Pricing trends
        avg(unit_price) as avg_selling_price,
        min(unit_price) as min_selling_price,
        max(unit_price) as max_selling_price,
        
        -- Timing
        min(ordered_at) as first_sale_at,
        max(ordered_at) as last_sale_at,
        
        -- Recent activity (last 30 days)
        sum(case when order_date >= current_date - 30 then quantity else 0 end) as units_sold_last_30_days,
        sum(case when order_date >= current_date - 30 then line_total else 0 end) as revenue_last_30_days

    from completed_sales
    group by product_id
),

-- Final product performance view
product_performance as (
    select
        -- Product info
        p.product_id,
        p.product_name,
        p.category,
        p.description,
        p.price as current_price,
        p.price_tier,
        
        -- Inventory
        p.stock_qty,
        p.stock_status,
        p.is_out_of_stock,
        
        -- Sales metrics (with defaults for products never sold)
        coalesce(s.times_ordered, 0) as times_ordered,
        coalesce(s.total_units_sold, 0) as total_units_sold,
        coalesce(s.total_revenue, 0) as total_revenue,
        s.avg_sale_amount,
        s.avg_selling_price,
        s.first_sale_at,
        s.last_sale_at,
        coalesce(s.units_sold_last_30_days, 0) as units_sold_last_30_days,
        coalesce(s.revenue_last_30_days, 0) as revenue_last_30_days,
        
        -- Calculated: days of stock remaining
        case 
            when coalesce(s.units_sold_last_30_days, 0) > 0 
            then round(p.stock_qty::numeric / (s.units_sold_last_30_days / 30.0), 0)
            else null
        end as days_of_stock_remaining,
        
        -- Product classification
        case
            when s.total_units_sold is null then 'no_sales'
            when s.units_sold_last_30_days >= 10 then 'bestseller'
            when s.units_sold_last_30_days >= 3 then 'steady'
            when s.last_sale_at < current_date - 60 then 'slow_mover'
            else 'moderate'
        end as sales_velocity,
        
        -- Inventory alerts
        case
            when p.stock_qty <= 0 then 'critical_out_of_stock'
            when p.stock_qty < 5 and coalesce(s.units_sold_last_30_days, 0) > 0 then 'reorder_urgent'
            when p.stock_qty < 20 then 'reorder_soon'
            else 'adequate'
        end as inventory_alert

    from products p
    left join product_sales s on p.product_id = s.product_id
)

select
    *,
    -- Rank products by revenue
    row_number() over (order by total_revenue desc) as revenue_rank,
    row_number() over (partition by category order by total_revenue desc) as category_rank,
    current_timestamp as dbt_updated_at
from product_performance
