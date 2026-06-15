"""
CDC Pipeline - Traffic Simulator
=================================
Generates realistic INSERT, UPDATE, and DELETE operations against
the source PostgreSQL database to test the CDC pipeline.

Usage:
    python scripts/simulate_traffic.py                    # Run with defaults (50 events, 0.5s interval)
    python scripts/simulate_traffic.py --events 100      # Generate 100 events
    python scripts/simulate_traffic.py --interval 0.1    # 100ms between events
    python scripts/simulate_traffic.py --continuous      # Run forever until Ctrl+C

What it does:
    - INSERTs new customers, products, and orders (simulates new business)
    - UPDATEs order statuses (simulates order lifecycle: pending → shipped → delivered)
    - UPDATEs product stock (simulates inventory changes)
    - DELETEs cancelled orders (simulates order cancellation)

This generates a realistic mix of operations that our CDC pipeline will capture.
"""

import argparse
import random
import sys
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


# ============================================================
# Configuration
# ============================================================
# Connection details match our docker-compose .env settings
DB_CONFIG = {
    "host": "127.0.0.1",      # Use IPv4 explicitly (localhost may resolve to IPv6)
    "port": 5434,             # Source PostgreSQL port (mapped from Docker)
    "dbname": "source_db",
    "user": "cdc_user",
    "password": "cdc_password",
}

# Probability distribution for operation types
# This mimics real-world traffic: mostly inserts/updates, few deletes
OPERATION_WEIGHTS = {
    "insert_customer": 15,
    "insert_order": 25,
    "update_order_status": 30,
    "update_product_stock": 15,
    "update_customer": 10,
    "delete_cancelled_order": 5,
}

# Fake data for generating realistic records
FIRST_NAMES = [
    "Aarav", "Priya", "Rohan", "Ananya", "Vivek", "Sneha", "Arjun", "Kavya",
    "Rahul", "Meera", "Aditya", "Nisha", "Karan", "Pooja", "Siddharth",
    "Divya", "Manish", "Riya", "Amit", "Shreya", "Varun", "Anjali",
]

LAST_NAMES = [
    "Patel", "Sharma", "Singh", "Kumar", "Gupta", "Joshi", "Verma", "Reddy",
    "Nair", "Iyer", "Chopra", "Mehta", "Shah", "Desai", "Rao", "Pillai",
    "Mishra", "Kapoor", "Chauhan", "Bhat", "Das", "Menon",
]

PRODUCT_NAMES = [
    "USB-C Hub", "Webcam HD", "Monitor Stand", "Ergonomic Mouse",
    "Cable Organizer", "Screen Protector", "Laptop Sleeve", "Mouse Pad XL",
    "Phone Stand", "Power Bank 20000mAh", "HDMI Cable 2m", "Ring Light",
]

PRODUCT_CATEGORIES = ["Electronics", "Office", "Accessories", "Home", "Sports", "Health"]

ADDRESSES = [
    "42 MG Road, Bangalore, KA 560001",
    "15 Park Street, Kolkata, WB 700016",
    "88 Marine Drive, Mumbai, MH 400020",
    "23 Connaught Place, New Delhi, DL 110001",
    "7 Anna Salai, Chennai, TN 600002",
    "31 Banjara Hills, Hyderabad, TS 500034",
    "56 FC Road, Pune, MH 411004",
    "12 CG Road, Ahmedabad, GJ 380009",
]

# Order status transitions (valid lifecycle)
STATUS_TRANSITIONS = {
    "pending": "confirmed",
    "confirmed": "shipped",
    "shipped": "delivered",
}


# ============================================================
# Database Operations
# ============================================================

def get_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


def insert_customer(conn):
    """Insert a new customer (simulates user registration)."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = f"{first.lower()}.{last.lower()}.{random.randint(1, 9999)}@email.com"
    phone = f"+91-{random.randint(7000000000, 9999999999)}"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customers (email, first_name, last_name, phone)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (email, first, last, phone),
        )
        customer_id = cur.fetchone()[0]
    conn.commit()
    return f"INSERT customer id={customer_id} ({first} {last})"


def insert_order(conn):
    """Insert a new order with 1-3 items (simulates a purchase)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Pick a random customer
        cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1;")
        customer = cur.fetchone()
        if not customer:
            return "SKIP: No customers exist yet"

        # Pick 1-3 random products
        num_items = random.randint(1, 3)
        cur.execute("SELECT id, price FROM products WHERE stock_qty > 0 ORDER BY RANDOM() LIMIT %s;", (num_items,))
        products = cur.fetchall()
        if not products:
            return "SKIP: No products in stock"

        # Calculate total
        total = sum(p["price"] * random.randint(1, 2) for p in products)
        address = random.choice(ADDRESSES)

        # Insert order
        cur.execute(
            """
            INSERT INTO orders (customer_id, status, total_amount, shipping_address)
            VALUES (%s, 'pending', %s, %s)
            RETURNING id;
            """,
            (customer["id"], total, address),
        )
        order_id = cur.fetchone()["id"]

        # Insert order items
        for product in products:
            qty = random.randint(1, 2)
            cur.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s);
                """,
                (order_id, product["id"], qty, product["price"]),
            )

            # Decrease stock (realistic side effect)
            cur.execute(
                "UPDATE products SET stock_qty = stock_qty - %s WHERE id = %s AND stock_qty >= %s;",
                (qty, product["id"], qty),
            )

    conn.commit()
    return f"INSERT order id={order_id} (customer={customer['id']}, items={len(products)}, total=${total:.2f})"


def update_order_status(conn):
    """Advance an order to its next status (simulates order lifecycle)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find an order that can be advanced
        cur.execute(
            """
            SELECT id, status FROM orders
            WHERE status IN ('pending', 'confirmed', 'shipped')
            ORDER BY RANDOM() LIMIT 1;
            """
        )
        order = cur.fetchone()
        if not order:
            return "SKIP: No orders to advance"

        new_status = STATUS_TRANSITIONS[order["status"]]
        cur.execute(
            "UPDATE orders SET status = %s WHERE id = %s;",
            (new_status, order["id"]),
        )
    conn.commit()
    return f"UPDATE order id={order['id']} status: {order['status']} → {new_status}"


def update_product_stock(conn):
    """Restock a product (simulates inventory replenishment)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, stock_qty FROM products ORDER BY stock_qty ASC LIMIT 1;")
        product = cur.fetchone()
        if not product:
            return "SKIP: No products exist"

        restock_qty = random.randint(10, 50)
        cur.execute(
            "UPDATE products SET stock_qty = stock_qty + %s WHERE id = %s;",
            (restock_qty, product["id"]),
        )
    conn.commit()
    return f"UPDATE product id={product['id']} ({product['name']}) stock: {product['stock_qty']} → {product['stock_qty'] + restock_qty}"


def update_customer(conn):
    """Update a customer's phone number (simulates profile update)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, first_name, last_name FROM customers ORDER BY RANDOM() LIMIT 1;")
        customer = cur.fetchone()
        if not customer:
            return "SKIP: No customers exist"

        new_phone = f"+91-{random.randint(7000000000, 9999999999)}"
        cur.execute(
            "UPDATE customers SET phone = %s WHERE id = %s;",
            (new_phone, customer["id"]),
        )
    conn.commit()
    return f"UPDATE customer id={customer['id']} ({customer['first_name']} {customer['last_name']}) phone → {new_phone}"


def delete_cancelled_order(conn):
    """Cancel and delete a pending order (simulates order cancellation)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Only delete pending orders (realistic — you can't cancel a delivered order)
        cur.execute(
            """
            SELECT id, customer_id, total_amount FROM orders
            WHERE status = 'pending'
            ORDER BY RANDOM() LIMIT 1;
            """
        )
        order = cur.fetchone()
        if not order:
            return "SKIP: No pending orders to cancel"

        # Delete order items first (CASCADE would handle this, but explicit is better for CDC)
        cur.execute("DELETE FROM order_items WHERE order_id = %s;", (order["id"],))
        # Delete the order
        cur.execute("DELETE FROM orders WHERE id = %s;", (order["id"],))
    conn.commit()
    return f"DELETE order id={order['id']} (customer={order['customer_id']}, amount=${order['total_amount']:.2f})"


# ============================================================
# Operation Dispatcher
# ============================================================

# Map operation names to functions
OPERATIONS = {
    "insert_customer": insert_customer,
    "insert_order": insert_order,
    "update_order_status": update_order_status,
    "update_product_stock": update_product_stock,
    "update_customer": update_customer,
    "delete_cancelled_order": delete_cancelled_order,
}


def pick_operation():
    """Choose a random operation based on weights."""
    operations = list(OPERATION_WEIGHTS.keys())
    weights = list(OPERATION_WEIGHTS.values())
    return random.choices(operations, weights=weights, k=1)[0]


# ============================================================
# Main Execution
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Traffic Simulator")
    parser.add_argument("--events", type=int, default=50, help="Number of events to generate (default: 50)")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between events (default: 0.5)")
    parser.add_argument("--continuous", action="store_true", help="Run continuously until Ctrl+C")
    args = parser.parse_args()

    print("=" * 60)
    print("CDC Pipeline - Traffic Simulator")
    print("=" * 60)
    print(f"  Target: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"  Events: {'∞ (continuous)' if args.continuous else args.events}")
    print(f"  Interval: {args.interval}s between events")
    print("=" * 60)
    print()

    # Test connection
    try:
        conn = get_connection()
        print("✓ Connected to source database")
    except psycopg2.OperationalError as e:
        print(f"✗ Cannot connect to database: {e}")
        print("  Make sure the postgres-source container is running:")
        print("  docker compose -f docker/docker-compose.yml --env-file .env up -d postgres-source")
        sys.exit(1)

    event_count = 0
    try:
        while True:
            event_count += 1

            # Pick and execute a random operation
            operation_name = pick_operation()
            operation_func = OPERATIONS[operation_name]

            try:
                result = operation_func(conn)
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f"  [{timestamp}] #{event_count:04d} | {result}")
            except psycopg2.Error as e:
                # Rollback on error and continue
                conn.rollback()
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] #{event_count:04d} | ERROR: {e.pgerror.strip() if e.pgerror else str(e)}")

            # Check if we've generated enough events
            if not args.continuous and event_count >= args.events:
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n\n{'=' * 60}")
        print(f"Stopped. Generated {event_count} events.")
        print(f"{'=' * 60}")
    finally:
        conn.close()

    print(f"\n✓ Completed: {event_count} events generated")
    print("  These changes are now in PostgreSQL's WAL, ready to be captured by CDC.")


if __name__ == "__main__":
    main()