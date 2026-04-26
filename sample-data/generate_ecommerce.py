#!/usr/bin/env python3
"""Generate sample e-commerce data."""

import os
import psycopg2
import random
from datetime import datetime, timedelta
from faker import Faker

DB_CONFIG = {
    'host': os.environ.get('SOURCE_DB_HOST', 'localhost'),
    'port': int(os.environ.get('SOURCE_DB_PORT', 5432)),
    'database': os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
    'user': os.environ.get('SOURCE_DB_USER', 'sourceuser'),
    'password': os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
}

fake = Faker()

def create_tables(conn):
    """Create database tables."""
    with conn.cursor() as cur:
        # Drop tables if they exist (clean start)
        cur.execute("DROP TABLE IF EXISTS order_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")

        # Customers table
        cur.execute("""
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(100) UNIQUE,
                address VARCHAR(200),
                city VARCHAR(100),
                state VARCHAR(100),
                zip_code VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Products table
        cur.execute("""
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                name VARCHAR(200),
                description TEXT,
                price DECIMAL(10, 2),
                category VARCHAR(50),
                stock_quantity INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Orders table
        cur.execute("""
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                customer_id INT REFERENCES customers(customer_id),
                order_date TIMESTAMP,
                total_amount DECIMAL(10, 2),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Order items table
        cur.execute("""
            CREATE TABLE order_items (
                item_id SERIAL PRIMARY KEY,
                order_id INT REFERENCES orders(order_id),
                product_id INT REFERENCES products(product_id),
                quantity INT,
                unit_price DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        print("✅ Tables created")

def generate_customers(conn, count=100):
    """Generate sample customers."""
    print(f"📦 Generating {count} customers...")

    with conn.cursor() as cur:
        for i in range(count):
            try:
                cur.execute("""
                    INSERT INTO customers (first_name, last_name, email, address, city, state, zip_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    fake.first_name()[:100],
                    fake.last_name()[:100],
                    fake.email()[:100],
                    fake.street_address()[:200],
                    fake.city()[:100],
                    fake.state()[:100],
                    fake.postcode()[:20],
                ))
            except Exception as e:
                print(f"  ⚠️  Skipping customer {i}: {e}")
                continue

    conn.commit()
    print(f"✅ Generated {count} customers")

def generate_products(conn, count=50):
    """Generate sample products."""
    print(f"📦 Generating {count} products...")

    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Toys']

    with conn.cursor() as cur:
        for _ in range(count):
            cur.execute("""
                INSERT INTO products (name, description, price, category, stock_quantity)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                fake.catch_phrase()[:200],
                fake.text()[:500],
                round(random.uniform(9.99, 999.99), 2),
                random.choice(categories),
                random.randint(0, 1000)
            ))

    conn.commit()
    print(f"✅ Generated {count} products")

def generate_orders(conn, count=10000):
    """Generate sample orders."""
    print(f"📦 Generating {count} orders...")

    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    start_date = datetime.now() - timedelta(days=365)

    with conn.cursor() as cur:
        # Get customer IDs and a price lookup dict for all products in one query each
        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT product_id, price FROM products")
        product_price = {row[0]: row[1] for row in cur.fetchall()}
        product_ids = list(product_price.keys())

        if not customer_ids or not product_ids:
            print("❌ No customers or products found!")
            return

        for i in range(count):
            customer_id = random.choice(customer_ids)
            order_date = start_date + timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            status = random.choice(statuses)

            # Insert order
            cur.execute("""
                INSERT INTO orders (customer_id, order_date, total_amount, status)
                VALUES (%s, %s, %s, %s)
                RETURNING order_id
            """, (customer_id, order_date, 0, status))

            order_id = cur.fetchone()[0]

            # Add 1-3 items to order
            num_items = random.randint(1, 3)
            total_amount = 0

            for _ in range(num_items):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 3)
                price = product_price[product_id]

                total_amount += float(price) * quantity

                cur.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, product_id, quantity, price))

            # Update order total
            cur.execute(
                "UPDATE orders SET total_amount = %s WHERE order_id = %s",
                (round(total_amount, 2), order_id),
            )

            if (i + 1) % 100 == 0:
                conn.commit()
                print(f"  ✓ Generated {i + 1}/{count} orders...")

    conn.commit()
    print(f"✅ Generated {count} orders")

def print_stats(conn):
    """Print database statistics."""
    print("\n📊 Database Statistics:")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        print(f"  👥 Customers: {cur.fetchone()[0]}")

        cur.execute("SELECT COUNT(*) FROM products")
        print(f"  📦 Products: {cur.fetchone()[0]}")

        cur.execute("SELECT COUNT(*) FROM orders")
        print(f"  🛒 Orders: {cur.fetchone()[0]}")

        cur.execute("SELECT COUNT(*) FROM order_items")
        print(f"  📝 Order Items: {cur.fetchone()[0]}")

        cur.execute("SELECT COALESCE(SUM(total_amount), 0)::numeric(10,2) FROM orders")
        total_revenue = cur.fetchone()[0]
        print(f"  💰 Total Revenue: ${total_revenue}")

def main():
    """Main function."""
    print("🚀 Starting data generation...\n")

    try:
        print("📡 Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected!\n")

        # create_tables(conn)
        print()

        generate_customers(conn, count=100)
        print()

        generate_products(conn, count=50)
        print()

        generate_orders(conn, count=10000)

        print_stats(conn)

        conn.close()

        print("\n✅ Data generation complete!")
        print("🎯 You can now create Airflow DAGs to process this data!\n")

    except psycopg2.Error as e:
        print(f"\n❌ Database Error: {e}")
        print("\n💡 Make sure Docker containers are running:")
        print("   docker-compose ps")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
