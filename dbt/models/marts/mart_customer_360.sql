-- ============================================================
-- Gold Layer: Customer 360 View
-- ============================================================
-- WHAT: Complete customer profile with lifetime metrics
-- WHY: Single source of truth for customer analytics
-- USED BY: Marketing, Customer Success, Sales teams
--
-- INTERVIEW TIP: This is a "wide table" pattern - denormalized
-- for fast analytical queries. Updates via full refresh.
-- ============================================================

{{ config(
    materialized='table',
    tags=['gold', 'mart', 'customer']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
    where is_valid_record = true
),

orders as (
    select * from {{ ref('stg_orders') }}
    where is_valid_record = true
),

-- Customer order history aggregation
customer_orders as (
    select
        customer_id,
        
        -- Order counts
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        count(case when is_cancelled then 1 end) as cancelled_orders,
        
        -- Revenue
        sum(total_amount) as lifetime_revenue,
        avg(total_amount) as avg_order_value,
        max(total_amount) as largest_order,
        
        -- Timing
        min(ordered_at) as first_order_at,
        max(ordered_at) as last_order_at,
        
        -- Recency (days since last order)
        extract(day from now() - max(ordered_at)) as days_since_last_order

    from orders
    group by customer_id
),

-- Final customer 360 view
customer_360 as (
    select
        -- Customer identifiers
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.full_name,
        c.phone_normalized as phone,
        
        -- Account info
        c.created_at as customer_since,
        c.account_age_days,
        
        -- Order history (with defaults for new customers)
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.completed_orders, 0) as completed_orders,
        coalesce(o.cancelled_orders, 0) as cancelled_orders,
        coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(o.avg_order_value, 0) as avg_order_value,
        o.largest_order,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.days_since_last_order, c.account_age_days) as days_since_last_order,
        
        -- Customer segments
        case
            when o.lifetime_revenue is null or o.lifetime_revenue = 0 then 'prospect'
            when o.lifetime_revenue < 100 then 'bronze'
            when o.lifetime_revenue < 500 then 'silver'
            when o.lifetime_revenue < 2000 then 'gold'
            else 'platinum'
        end as customer_tier,
        
        -- Engagement status
        case
            when o.total_orders is null then 'never_ordered'
            when o.days_since_last_order <= 30 then 'active'
            when o.days_since_last_order <= 90 then 'at_risk'
            when o.days_since_last_order <= 180 then 'dormant'
            else 'churned'
        end as engagement_status,
        
        -- Repeat customer flag
        coalesce(o.total_orders, 0) > 1 as is_repeat_customer,
        
        -- High value flag
        coalesce(o.lifetime_revenue, 0) >= 500 as is_high_value

    from customers c
    left join customer_orders o on c.customer_id = o.customer_id
)

select
    *,
    current_timestamp as dbt_updated_at
from customer_360
