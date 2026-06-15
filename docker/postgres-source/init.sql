-- ============================================================
-- CDC Pipeline - Source Database Initialization Script
-- ============================================================
-- This script runs automatically on first container startup.
-- It creates the OLTP schema, enables logical replication,
-- and seeds initial data for testing.
-- ============================================================

-- ============================================================
-- 1. GRANT REPLICATION PRIVILEGE
-- ============================================================
-- The cdc_user needs REPLICATION privilege to create replication slots
-- and read the WAL stream. Without this, our CDC producer can't connect.
ALTER USER cdc_user WITH REPLICATION;

-- ============================================================
-- 2. CREATE SCHEMA AND TABLES
-- ============================================================
-- We're modeling a simplified e-commerce system with 4 tables.
-- This gives us enough complexity to demonstrate multi-table CDC
-- with foreign key relationships and various operation types.

-- Customers table: stores registered users
CREATE TABLE IF NOT EXISTS customers (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Products table: items available for purchase
CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    price           DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    stock_qty       INTEGER NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
    description     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders table: purchase transactions
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    total_amount    DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    shipping_address TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Order items table: individual line items within an order
CREATE TABLE IF NOT EXISTS order_items (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id      INTEGER NOT NULL REFERENCES products(id),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- 3. CREATE INDEXES
-- ============================================================
-- Indexes speed up queries but also appear in CDC events metadata.
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_customers_email ON customers(email);

-- ============================================================
-- 4. SET REPLICA IDENTITY FULL
-- ============================================================
-- REPLICA IDENTITY controls what data is included in WAL for
-- UPDATE and DELETE operations.
--
-- DEFAULT = only primary key in old row image
-- FULL    = ALL columns in old row image
--
-- We use FULL because our consumer needs complete "before" state
-- for proper change tracking, soft deletes, and audit trails.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- ============================================================
-- 5. CREATE PUBLICATION
-- ============================================================
-- A publication defines which tables emit CDC events.
-- We include all 4 tables and all operations (INSERT, UPDATE, DELETE).
CREATE PUBLICATION cdc_publication FOR TABLE customers, products, orders, order_items;

-- ============================================================
-- 6. CREATE REPLICATION SLOT
-- ============================================================
-- The replication slot is our "bookmark" in the WAL.
-- - 'cdc_slot' = name we'll reference in our CDC producer
-- - 'pgoutput' = built-in logical decoding output plugin
--
-- This MUST be created after the publication because pgoutput
-- uses publications to know which tables to decode.
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

-- ============================================================
-- 7. SEED DATA - Customers
-- ============================================================
-- Initial test data so we have something to work with immediately.
INSERT INTO customers (email, first_name, last_name, phone) VALUES
    ('alice.johnson@email.com', 'Alice', 'Johnson', '+1-555-0101'),
    ('bob.smith@email.com', 'Bob', 'Smith', '+1-555-0102'),
    ('carol.williams@email.com', 'Carol', 'Williams', '+1-555-0103'),
    ('david.brown@email.com', 'David', 'Brown', '+1-555-0104'),
    ('eva.davis@email.com', 'Eva', 'Davis', '+1-555-0105'),
    ('frank.miller@email.com', 'Frank', 'Miller', '+1-555-0106'),
    ('grace.wilson@email.com', 'Grace', 'Wilson', '+1-555-0107'),
    ('henry.moore@email.com', 'Henry', 'Moore', '+1-555-0108'),
    ('iris.taylor@email.com', 'Iris', 'Taylor', '+1-555-0109'),
    ('jack.anderson@email.com', 'Jack', 'Anderson', '+1-555-0110');

-- ============================================================
-- 8. SEED DATA - Products
-- ============================================================
INSERT INTO products (name, category, price, stock_qty, description) VALUES
    ('Wireless Headphones', 'Electronics', 79.99, 150, 'Bluetooth over-ear headphones with noise cancellation'),
    ('Running Shoes', 'Sports', 129.99, 80, 'Lightweight running shoes with cushioned sole'),
    ('Coffee Maker', 'Kitchen', 49.99, 200, '12-cup programmable coffee maker'),
    ('Backpack', 'Accessories', 59.99, 120, 'Water-resistant laptop backpack 15.6 inch'),
    ('Yoga Mat', 'Sports', 29.99, 300, 'Non-slip exercise yoga mat 6mm thick'),
    ('Desk Lamp', 'Home', 34.99, 175, 'LED desk lamp with adjustable brightness'),
    ('Water Bottle', 'Accessories', 24.99, 500, 'Insulated stainless steel water bottle 750ml'),
    ('Bluetooth Speaker', 'Electronics', 44.99, 90, 'Portable waterproof bluetooth speaker'),
    ('Notebook Set', 'Office', 14.99, 400, 'Set of 3 hardcover lined notebooks'),
    ('Phone Case', 'Electronics', 19.99, 600, 'Shockproof clear phone case'),
    ('Resistance Bands', 'Sports', 22.99, 250, 'Set of 5 resistance bands with handles'),
    ('Mechanical Keyboard', 'Electronics', 89.99, 60, 'RGB mechanical keyboard with Cherry MX switches'),
    ('Plant Pot', 'Home', 16.99, 350, 'Ceramic plant pot with drainage hole'),
    ('Sunglasses', 'Accessories', 39.99, 180, 'UV400 polarized sunglasses'),
    ('Protein Powder', 'Health', 54.99, 100, 'Whey protein powder chocolate flavor 2lb');

-- ============================================================
-- 9. SEED DATA - Orders (with various statuses to test UPDATEs)
-- ============================================================
INSERT INTO orders (customer_id, status, total_amount, shipping_address) VALUES
    (1, 'delivered', 159.98, '123 Oak Street, Springfield, IL 62701'),
    (2, 'shipped', 129.99, '456 Maple Ave, Portland, OR 97201'),
    (3, 'confirmed', 84.98, '789 Pine Road, Austin, TX 78701'),
    (4, 'pending', 49.99, '321 Elm Drive, Denver, CO 80201'),
    (5, 'delivered', 54.98, '654 Cedar Lane, Seattle, WA 98101');

-- ============================================================
-- 10. SEED DATA - Order Items
-- ============================================================
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 79.99),   -- Alice: Wireless Headphones
    (1, 1, 1, 79.99),   -- Alice: another pair (gift)
    (2, 2, 1, 129.99),  -- Bob: Running Shoes
    (3, 4, 1, 59.99),   -- Carol: Backpack
    (3, 7, 1, 24.99),   -- Carol: Water Bottle
    (4, 3, 1, 49.99),   -- David: Coffee Maker
    (5, 5, 1, 29.99),   -- Eva: Yoga Mat
    (5, 7, 1, 24.99);   -- Eva: Water Bottle

-- ============================================================
-- 11. CREATE UPDATED_AT TRIGGER FUNCTION
-- ============================================================
-- Automatically updates the updated_at timestamp whenever a row
-- is modified. This is standard practice in OLTP systems.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables with updated_at column
CREATE TRIGGER trigger_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- DONE! Source database is ready for CDC.
-- ============================================================
-- Summary:
--   ✓ 4 tables created with proper constraints
--   ✓ REPLICA IDENTITY FULL set on all tables
--   ✓ Publication 'cdc_publication' created
--   ✓ Replication slot 'cdc_slot' created (pgoutput plugin)
--   ✓ Seed data inserted (10 customers, 15 products, 5 orders)
--   ✓ updated_at triggers installed
-- ============================================================
