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
                    SELECT product_id,category_id,price
                    FROM products
            """)
            product_detail = curr.fetchall()
            logger.info("Fetch successfully product details")
    except Exception as e :
            logger.info("Fetch unsuccessfully product details")

    return product_detail

product_detail = list_products()


product_id = [product_id for product_id,_,_ in product_detail]

product_to_category = {product_id: category_id for product_id, category_id, price in product_detail}
product_to_price = {product_id: price for product_id, category_id, price in product_detail}


def order_items(order_id: str):
    "Pick random items and random quantity basing weight of category"

    quantity_item = random.choices([1,2,3], weights= [0.9,0.25,0.25])[0]

    order = []

    for i in range(1,quantity_item +1):
        product= random.choice(product_id)
        product_quantity = random.choices([1,2,3], weights= [0.95,0.05,0.05])[0]
        category = product_to_category[product]
        price = product_to_price[product]

        order_item_id = f"{order_id}-{i}"

        order.append({
            "order_item_id": order_item_id,
            "product_id" : product,
            "category_id" : category,
            "quantity" : product_quantity,
            "price": float(price)
        })
    return order


def generate_fake_data():


    order_date = faker.date_time_between(start_date='-1y', end_date='now')
    date_str = order_date.strftime("%Y%m%d")

    # Increment or reset counter for this date
    if date_str not in order_counters:
        order_counters[date_str] = 1
    else:
        order_counters[date_str] += 1

    seq_num = order_counters[date_str]


    order_id = "ORD" + order_date.strftime("%Y%m%d") + "-" + str(seq_num)
    order_detail = order_items(order_id)
    total_amount = sum(item["price"] * item["quantity"] for item in order_detail)

    return {
        "order_id": order_id,
        "user_id": random.randint(1, 1000000),
        "order_detail" : order_detail,
        "store_id": random.randint(1, 1000),
        "total_amount": total_amount,
        "created_at" : order_date
    }

orders_topic = "orders_stream"
order_items_topic = "order_items_stream"

Batch_size = 100


def producer_stream():
    producer = Producer({"bootstrap.servers": "localhost:9094"})
    while True:
        try:
            message = generate_fake_data()
            # orders_message = {
            #     "order_id": message["order_id"],
            #     "user_id": message["user_id"],
            #     "store_id": message["store_id"],
            #     "total_amount": message["total_amount"],
            #     "order_date": message["created_at"]
            # }
            orders_message = {
                "schema": {
                    "type": "struct",
                    "fields": [
                        {"field": "order_id", "type": "string"},
                        {"field": "user_id", "type": "int32"},
                        {"field": "store_id", "type": "int32"},
                        {"field": "total_amount", "type": "float"},
                        {"field": "order_date", "type": "string"}
                    ],
                    "optional": False,
                    "name": "orders"
                },
                "payload": {
                    "order_id": message["order_id"],
                    "user_id": message["user_id"],
                    "store_id": message["store_id"],
                    "total_amount": message["total_amount"],
                    "order_date": message["created_at"]
                }
                }
            producer.produce(orders_topic, json.dumps(orders_message,default=str))
            for item in message["order_detail"]:
                order_item_message = {
                    "order_item_id": item["order_item_id"],
                    "order_id": message["order_id"],
                    "product_id": item["product_id"],
                    "quantity": item["quantity"],
                    "price": item["price"]
                }
                producer.produce(order_items_topic, json.dumps(order_item_message,default=str))


            producer.flush()
            time.sleep(1)

        except Exception as e:
            logger.error("Failed to produce message: %s", e)
            time.sleep(1)

producer_stream()

# producer = Producer({"bootstrap.servers": "localhost:9094"})
# record_order = {
#     "schema": {
#         "type": "struct",
#         "fields": [
#             {"field": "order_id", "type": "string"},
#             {"field": "user_id", "type": "int32"},
#             {"field": "store_id", "type": "int32"},
#             {"field": "total_amount", "type": "float"},
#             {"field": "order_date", "type": "string"}
#         ],
#         "optional": False,
#         "name": "orders"
#     },
#     "payload": {
#         "order_id": "ORD20251003-1",
#         "user_id": 1,
#         "store_id": 1,
#         "total_amount": 100000.18,
#         "order_date": "2025-10-02T20:30:00"
#     }
# }
# record_items = {
#     "schema": {
#         "type": "struct",
#         "fields": [
#             {"field": "order_item_id", "type": "string"},
#             {"field": "order_id", "type": "string"},
#             {"field": "product_id", "type": "int32"},
#             {"field": "quantity", "type": "int32"},
#             {"field": "price", "type": "float"}
#         ],
#         "optional": False,
#         "name": "order_items"
#     },
#     "payload": {
#         "order_item_id": "ORD20251003-1-1",
#         "order_id": "ORD20251003-1",
#         "product_id": 1,
#         "quantity": 3,
#         "price": 100000.18
#     }
# }
# producer.produce(
#     topic="orders_stream",
#     value=json.dumps(record_order).encode("utf-8")  # encode JSON to bytes
# )

# producer.produce(
#     topic="order_items_stream",
#     value=json.dumps(record_items).encode("utf-8")  # encode JSON to bytes
# )

# producer.flush()
# print("Record sent successfully!")