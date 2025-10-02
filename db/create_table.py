import psycopg2
from config.app_config import load_config
from db.get_db_connection import get_db_connection
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CUSTOMERS_TABLE = """
CREATE TABLE IF NOT EXISTS customers (
    user_id SERIAL PRIMARY KEY,
    user_name TEXT NOT NULL,
    user_tier TEXT,
    user_identity_card_number TEXT,
    phone TEXT,
    address TEXT,
    update_at DATE
);
"""

CATEGORIES_TABLE = """
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT NOT NULL
);
"""

PRODUCTS_TABLE = """
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category_id INTEGER REFERENCES categories(category_id),
    price NUMERIC(12,2),
    stock_quantity INTEGER,
    brand TEXT
);
"""

STORES_TABLE = """
CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    store_address TEXT,
    city TEXT
);
"""

ORDER_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES customers(user_id),
    store_id INTEGER REFERENCES stores(store_id),
    order_date DATE,
    total_amount NUMERIC(12,2)
);
"""

ORDER_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    price NUMERIC(12,2)
);
"""


def create_tables():
    db_config = load_config('postgresql')
    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(CUSTOMERS_TABLE)
            cur.execute(PRODUCTS_TABLE)
            cur.execute(CATEGORIES_TABLE)
            cur.execute(STORES_TABLE)
            conn.commit()
            logger.info("Tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        conn.close()

def loading_data():
    db_config = load_config('postgresql')
    conn = get_db_connection(db_config)

    try:
        with conn.cursor() as cur:
            with open("data/customers.csv", "r") as f:
                cur.copy_expert("COPY customers FROM STDIN WITH CSV HEADER", f)
            with open("data/categories.csv", "r") as f:
                cur.copy_expert("COPY categories FROM STDIN WITH CSV HEADER", f)
            with open("data/products.csv", "r") as f:
                cur.copy_expert("COPY products (product_id, name, category_id, price, stock_quantity, brand) FROM STDIN WITH CSV HEADER", f)
            with open("data/stores.csv", "r") as f:
                cur.copy_expert("COPY stores FROM STDIN WITH CSV HEADER", f)

        conn.commit()
        logger.info("CSV files loaded directly into tables.")
    except Exception:
        logger.exception("Error loading data")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    create_tables()
    loading_data()
