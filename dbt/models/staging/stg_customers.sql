-- ============================================================
-- Silver Layer: stg_customers
-- ============================================================
-- WHAT: Cleaned, standardized customer data
-- WHY: Consistent naming, proper types, business rules applied
-- FROM: CDC-replicated customers table (Bronze)
-- ============================================================

-- SIMPLE EXPLANATION:
-- Think of this as a "clean copy" of the raw customer data.
-- We fix formatting, add full_name, and filter out bad records.

with source as (
    select * from {{ source('cdc', 'customers') }}
),

cleaned as (
    select
        -- Primary key
        id as customer_id,
        
        -- Contact info (trim whitespace, lowercase email)
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        
        -- Derived field: full name for display
        trim(first_name) || ' ' || trim(last_name) as full_name,
        
        -- Phone (normalize format)
        regexp_replace(phone, '[^0-9+]', '', 'g') as phone_normalized,
        phone as phone_original,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Derived: account age in days
        extract(day from now() - created_at) as account_age_days,
        
        -- Data quality flag
        case 
            when email is null or email = '' then false
            when first_name is null or first_name = '' then false
            else true
        end as is_valid_record

    from source
    where id is not null  -- Filter corrupt records
)

select * from cleaned
