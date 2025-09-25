import json
import time
import random
from faker import Faker
from confluent_kafka import Producer
import pandas as pd

faker = Faker()

def generate_fake_user_data():
    data = [
        {
            "user_id": i,
            "user_name": faker.name(),
            "user_tier": random.choice(["gold", "silver", "bronze"]),
            "user_identity_card_number": ''.join(str(random.randint(0, 9)) for _ in range(12)),
            "phone": faker.phone_number(),
            "address": random.choice([
                "Ho Chi Minh", "Hanoi", "Da Nang", "Hoi An", "Nha Trang",
                "Da Lat", "Hue", "Can Tho", "Bien Hoa", "Vung Tau"
            ]),
            "update_at": faker.date_between(start_date='-1y', end_date='today')
        }
        for i in range(1, 1000000)
    ]
    df = pd.DataFrame(data)
    return df.to_csv("customers.csv", index=False)


def generate_fake_store_data():
    data = [
        {
            "store_id": i,
            "name": "Dien may xanh",
            "store_address" : "".join((str(random.randint(100,200))," Duong so ",str(random.randint(1,9)))) ,
            "city": random.choices(
                ["Ho Chi Minh", "Hanoi", "Da Nang", "Hoi An", "Nha Trang",
                "Da Lat", "Hue", "Can Tho", "Bien Hoa", "Vung Tau"],
                weights = [0.75,0.75,0.5,0.25,0.25,0.25,0.25,0.25,0.25,0.25],
                k = 1)[0]
        }
        for i in range(1, 1000)
    ]
    df = pd.DataFrame(data)
    return df.to_csv("stores.csv", index=False)


def generate_fake_product_data(num_products=1000):
    """
    Generate a list of fake product data for an electrical store.
    Each product has: product_id, name, category, price, stock_quantity, brand.
    """

    fake = Faker()
    categories = [
        "Laptop", "Smartphone", "Tablet", "Television", "Headphones",
        "Camera", "Smartwatch", "Speaker", "Printer", "Monitor"
    ]
    brands = [
        "Sony", "Samsung", "Apple", "LG", "Dell", "HP", "Lenovo", "Canon", "Bose", "Asus"
    ]
    products = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        brand = random.choice(brands)
        name = f"{brand} {category} {random.randint(1,100)}"
        price = round(random.uniform(1000, 20000)*1000, 2)
        stock_quantity = random.randint(0, 100)
        product = {
            "product_id": i,
            "name": name,
            "category": category,
            "price": price,
            "stock_quantity": stock_quantity,
            "brand": brand
        }
        products.append(product)

    return pd.DataFrame(products).to_csv("products.csv",index=False)

