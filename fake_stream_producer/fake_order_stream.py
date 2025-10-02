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

# Track order counters per date
order_counters = {}


def list_products():
    db_config = load_config('postgresql')
    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as curr:
            curr.execute("""
                    SELECT name,category_name,price
                    FROM products
                    LEFT JOIN categories ON products.category_id = categories.category_id
            """)
            product_detail = curr.fetchall()
            logger.info("Fetch successfully product details")
    except Exception as e :
            logger.info("Fetch unsuccessfully product details")

    return product_detail

product_detail = list_products()

product_name = [name for name,_,_ in product_detail]

product_to_category = {name: category_name for name, category_name, price in product_detail}
product_to_price = {name: price for name, category_name, price in product_detail}


def order_items(order_id: str):
    "Pick random items and random quantity basing weight of category"

    quantity_item = random.choices([1,2,3], weights= [0.9,0.25,0.25])[0]

    order = []

    for i in range(1,quantity_item +1):
        product= random.choice(product_name)
        product_quantity = random.choices([1,2,3], weights= [0.95,0.05,0.05])
        category = product_to_category[product]
        price = product_to_price[product]

        order_item_id = f"{order_id}-{i}"

        order.append({
            "order_item_id": order_item_id,
            "product" : product,
            "category" : category,
            "quantity" : product_quantity,
            "price": float(price)
        })
    return order


def generate_fake_data():


    order_date = faker.date_between(start_date='-1y', end_date='today')
    date_str = order_date.strftime("%Y%m%d")

    # Increment or reset counter for this date
    if date_str not in order_counters:
        order_counters[date_str] = 1
    else:
        order_counters[date_str] += 1

    seq_num = order_counters[date_str]


    order_id = "ORD" + order_date.strftime("%Y%m%d") + "-" + str(seq_num)
    order_detail = order_items(order_id)

    return {
        "order_id": order_id,
        "user_id": random.randint(1, 1000000),
        "order_detail" : order_detail,
        "store_id": random.randint(1, 1000),
        "created_at" : order_date
    }

for _ in range(5):
    print(generate_fake_data())