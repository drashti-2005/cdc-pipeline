-- ============================================================
-- Gold Layer: Daily Revenue Summary
-- ============================================================
-- WHAT: Daily aggregated revenue metrics
-- WHY: Fast queries for revenue dashboards and reports
-- USED BY: Finance team, executives, BI dashboards
--
-- INTERVIEW TIP: Gold layer tables are denormalized for fast reads.
-- Trade-off: More storage, but sub-second query times.
-- ============================================================

{{ config(
    materialized='table',
    tags=['gold', 'mart', 'finance']
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where is_valid_record = true
),

daily_metrics as (
    select
        -- Time dimension
        order_date,
        order_year,
        order_month,
        
        -- Revenue metrics
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        count(case when is_cancelled then 1 end) as cancelled_orders,
        count(case when is_active then 1 end) as active_orders,
        
        -- Financial metrics
        sum(total_amount) as gross_revenue,
        sum(case when is_completed then total_amount else 0 end) as realized_revenue,
        sum(case when is_cancelled then total_amount else 0 end) as lost_revenue,
        
        -- Averages
        avg(total_amount) as avg_order_value,
        
        -- Order size distribution
        count(case when order_size = 'small' then 1 end) as small_orders,
        count(case when order_size = 'medium' then 1 end) as medium_orders,
        count(case when order_size = 'large' then 1 end) as large_orders,
        count(case when order_size = 'enterprise' then 1 end) as enterprise_orders,
        
        -- Unique customers
        count(distinct customer_id) as unique_customers,
        
        -- Fulfillment metrics
        avg(fulfillment_hours) as avg_fulfillment_hours

    from orders
    group by order_date, order_year, order_month
)

select
    *,
    -- Calculated rates
    round(100.0 * completed_orders / nullif(total_orders, 0), 2) as completion_rate,
    round(100.0 * cancelled_orders / nullif(total_orders, 0), 2) as cancellation_rate,
    
    -- Running totals (useful for dashboards)
    sum(gross_revenue) over (
        partition by order_year, order_month 
        order by order_date
    ) as mtd_revenue,
    
    -- Audit columns
    current_timestamp as dbt_updated_at

from daily_metrics
order by order_date desc
