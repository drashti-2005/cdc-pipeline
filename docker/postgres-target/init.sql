-- ============================================================
-- Target PostgreSQL - Schema Initialization
-- ============================================================
-- This creates the SAME schema as the source database so CDC events
-- can be replicated. No CDC setup here - it's a read replica.

-- ============================================================
-- Schema: Same tables as source, no replication configuration
-- ============================================================

-- Customers table (MUST match source exactly)
CREATE TABLE IF NOT EXISTS customers (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Products table (MUST match source exactly)
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    price           DECIMAL(10, 2) NOT NULL,
    stock_qty       INTEGER NOT NULL DEFAULT 0,
    description     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders table (NO FK constraint - CDC events may arrive out of order)
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,  -- No FK for CDC compatibility
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_amount    DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    shipping_address TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Order items table (NO FK constraints - CDC events may arrive out of order)
CREATE TABLE IF NOT EXISTS order_items (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL,   -- No FK for CDC compatibility
    product_id      INTEGER NOT NULL,   -- No FK for CDC compatibility
    quantity        INTEGER NOT NULL,
    unit_price      DECIMAL(10, 2) NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- Indexes for performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- ============================================================
-- Grant permissions
-- ============================================================

-- Grant all privileges to postgres user (default user in docker)
-- This is simpler than source since we don't need replication privileges

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- ============================================================
-- Verification
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Target PostgreSQL initialization complete!';
    RAISE NOTICE 'Tables: customers, products, orders, order_items';
    RAISE NOTICE 'Ready to receive CDC events';
    RAISE NOTICE '============================================';
END $$;
