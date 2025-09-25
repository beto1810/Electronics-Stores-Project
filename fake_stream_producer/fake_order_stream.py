import json
import time
import random
from unicodedata import category
from faker import Faker
from confluent_kafka import Producer
from config.app_config import load_config
from db.get_db_connection import get_db_connection
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

faker = Faker()


def list_products():
    db_config = load_config('postgresql')
    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as curr:
            curr.execute("""
                    SELECT name,category,price
                    FROM products
            """)
            product_detail = curr.fetchall()
            logger.info("Fetch successfully product details")
    except Exception as e :
            logger.info("Fetch unsuccessfully product details")

    return product_detail

product_detail = list_products()

product_name = [name for name,_,_ in product_detail]

product_to_category = {name: category for name, category, price in product_detail}
product_to_price = {name: price for name, category, price in product_detail}

print(product_to_price)

def order_items():
    "Pick random items and random quantity basing weight of category"

    quantity_item = random.choices([1,2,3], weights= [0.9,0.25,0.25])[0]

    order = []

    for _ in range(quantity_item):
        product= random.choice(product_name)
        product_quantity = random.choices([1,2,3], weights= [0.95,0.05,0.05])
        category = product_to_category[product]
        price = product_to_price[product]

        order.append({
            "product" : product,
            "category" : category,
            "quantity" : product_quantity,
            "price": price
        })
    return order

print(order_items())















def generate_fake_data():
    return {
        "event_id": f"evt-{int(time.time()*1000)}-{i}",
        "order_id": faker.uuid4,
        "user_id": random.randint(100000, 1000000),
        "product_name": random.choice(["Laptop", "Smartphone", "Tablet", "Smartwatch", "Headphones", "Keyboard", "Mouse", "Speaker", "Monitor", "Printer"]),
        "product_price": round(random.random()*1000, 2),
        "product_quantity" : random.randint(1, 10),
        "product_currency" : "VND",
        "store_id": random.randint(1, 10),
        "created_at" : time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
    }