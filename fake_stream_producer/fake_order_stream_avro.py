"""
Enhanced order stream producer with Avro serialization and Schema Registry support
"""

import json
import time
import random
import uuid
from decimal import Decimal
from typing import Dict, List, Any
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import logger
from db.get_db_connection import get_db_connection
from config.app_config import load_config
from schemas.schema_registry_client import (
    SchemaRegistryClient as CustomSchemaRegistryClient,
    load_avro_schema_from_file
)


faker = Faker()


class AvroOrderProducer:
    """Enhanced order producer with Avro serialization support"""

    def __init__(self):
        # Load configurations
        self.kafka_config = load_config('kafka_server')
        self.db_config = load_config('postgresql')

        # Schema Registry setup
        self.schema_registry_url = "http://localhost:8081"
        self.schema_registry_client = SchemaRegistryClient({
            'url': self.schema_registry_url
        })

        # Load and register schemas
        self.order_schema = load_avro_schema_from_file("schemas/order.avsc")

        # Create Avro serializer
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            json.dumps(self.order_schema),
            self.order_to_dict
        )

        # Setup Kafka producer with Avro support
        producer_config = {
            'bootstrap.servers': self.kafka_config.get('bootstrap.servers', 'localhost:9094'),
            'client.id': 'avro-order-producer',
            'acks': 'all',
            'retries': 5,
            'batch.size': 16384,
            'linger.ms': 10,
        }

        self.producer = Producer(producer_config)

        # Load product data
        self.product_details = self._load_products()
        self._setup_product_lookups()

        # Configuration
        self.RATE_PER_SEC = 50
        self.BATCH = 10
        self.topic = "order_stream_avro"
        self.DLQ_TOPIC = "order_stream_avro_dlq"

        # Simulation settings
        self.SIMULATE_FAILURE = True
        self.FAILURE_RATE = 0.01  # 1%

    def _load_products(self) -> List[tuple]:
        """Load product details from database"""
        conn = get_db_connection(self.db_config)
        try:
            with conn.cursor() as curr:
                curr.execute("""
                    SELECT p.name, p.category_id, c.category_name, p.price
                    FROM products p
                    JOIN categories c ON p.category_id = c.category_id
                """)
                product_detail = curr.fetchall()
                logger.info("Fetched product details successfully. Count: %d", len(product_detail))
                return product_detail
        except Exception as e:
            logger.error("Failed to fetch product details: %s", e)
            return []
        finally:
            conn.close()

    def _setup_product_lookups(self):
        """Setup product lookup dictionaries"""
        self.product_names = [name for name, _, _, _ in self.product_details]
        self.product_to_category_id = {name: category_id for name, category_id, _, _ in self.product_details}
        self.product_to_category_name = {name: category_name for name, _, category_name, _ in self.product_details}
        self.product_to_price = {name: float(price) for name, _, _, price in self.product_details}

    @staticmethod
    def order_to_dict(order, ctx):
        """
        Serialize order object to dictionary for Avro
        This function is called by AvroSerializer
        """
        return order

    def _decimal_to_bytes(self, decimal_value: float, precision: int = 12, scale: int = 2) -> bytes:
        """Convert decimal value to bytes for Avro decimal logical type"""
        try:
            # Convert to Decimal with proper scale
            decimal_obj = Decimal(str(decimal_value)).quantize(Decimal('0.01'))

            # Convert to integer (multiply by 10^scale)
            int_value = int(decimal_obj * (10 ** scale))

            # Calculate byte length needed, with a reasonable maximum
            if int_value == 0:
                byte_length = 1
            else:
                bit_length = int_value.bit_length()
                if int_value < 0:
                    bit_length += 1  # Add sign bit
                byte_length = (bit_length + 7) // 8

                # Cap the byte length to avoid overflow (16 bytes should be plenty)
                if byte_length > 16:
                    logger.warning(f"Decimal value {decimal_value} too large, capping to maximum size")
                    # Scale down the value if it's too large
                    max_int = (1 << (16 * 8 - 1)) - 1  # Max value for 16 bytes signed
                    if int_value > max_int:
                        int_value = max_int
                    elif int_value < -max_int:
                        int_value = -max_int
                    byte_length = 16

            return int_value.to_bytes(byte_length, byteorder='big', signed=True)

        except (ValueError, OverflowError) as e:
            logger.error(f"Failed to convert decimal {decimal_value} to bytes: {e}")
            # Fallback: return zero as bytes
            return (0).to_bytes(8, byteorder='big', signed=True)

    def _generate_order_items(self) -> List[Dict[str, Any]]:
        """Generate random order items"""
        if not self.product_names:
            logger.warning("No products available, generating empty order")
            return []

        quantity_items = random.choices([1, 2, 3], weights=[0.9, 0.25, 0.25])[0]
        order_items = []

        for _ in range(quantity_items):
            product = random.choice(self.product_names)
            product_quantity = random.choices([1, 2, 3], weights=[0.95, 0.05, 0.05])[0]
            category_id = self.product_to_category_id[product]
            category_name = self.product_to_category_name[product]
            price = self.product_to_price[product]

            order_items.append({
                "product": product,
                "category_id": category_id,
                "category_name": category_name,
                "quantity": product_quantity,
                "price": float(price)
            })

        return order_items

    def generate_order(self) -> Dict[str, Any]:
        """Generate a fake order with proper Avro formatting"""
        items = self._generate_order_items()
        sub_total = sum(item['quantity'] * self.product_to_price.get(item['product'], 0) for item in items)

        # Convert timestamp to milliseconds since epoch
        created_at_ms = int(time.time() * 1000)

        return {
            "order_id": f"ord-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}",
            "user_id": random.randint(100000, 1000000),
            "store_id": random.randint(1, 10),
            "created_at": created_at_ms,
            "items": items,
            "sub_total": float(sub_total)
        }

    def deliver_report(self, err, msg):
        """Delivery report callback"""
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            logger.info(
                "Delivered to topic=%s partition=%s offset=%s",
                msg.topic(), msg.partition(), msg.offset()
            )

    def send_to_dlq(self, original_record: Dict, error: str):
        """Send failed record to Dead Letter Queue"""
        dlq_record = {
            "original": original_record,
            "error": error,
            "failed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())
        }

        try:
            self.producer.produce(
                self.DLQ_TOPIC,
                key=str(original_record.get("order_id")).encode("utf-8"),
                value=json.dumps(dlq_record).encode("utf-8"),
                callback=self.deliver_report
            )
            logger.info("Sent failed record to DLQ for order_id=%s", original_record.get("order_id"))
        except Exception as dlq_err:
            logger.critical(
                "Failed to produce to DLQ for order_id=%s: %s",
                original_record.get("order_id"), dlq_err
            )

    def produce_order(self, order: Dict[str, Any]):
        """Produce a single order to Kafka with Avro serialization"""
        try:
            if self.SIMULATE_FAILURE and random.random() < self.FAILURE_RATE:
                raise RuntimeError("Simulated produce failure")

            # Use string key (order_id)
            key = str(order["order_id"])

            # Serialize and produce with Avro
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=self.avro_serializer(order, SerializationContext(self.topic, MessageField.VALUE)),
                callback=self.deliver_report
            )

            # Trigger delivery reports
            self.producer.poll(0)

        except Exception as e:
            logger.error(
                "Failed to produce message for user_id=%s order_id=%s: %s",
                order.get("user_id"), order.get("order_id"), e
            )
            self.send_to_dlq(order, str(e))

    def run_producer(self):
        """Run the producer loop"""
        interval = 1.0 / self.RATE_PER_SEC
        produced = 0

        logger.info("Starting Avro order producer...")
        logger.info(f"Target rate: {self.RATE_PER_SEC} messages/sec")
        logger.info(f"Batch size: {self.BATCH}")
        logger.info(f"Topic: {self.topic}")

        try:
            while True:
                start = time.time()

                # Generate batch of orders
                batch = [self.generate_order() for _ in range(self.BATCH)]

                # Produce each order in the batch
                for order in batch:
                    self.produce_order(order)
                    produced += 1

                # Rate limiting
                elapsed = time.time() - start
                sleep_time = max(0, interval * self.BATCH - elapsed)
                time.sleep(sleep_time)

                # Log progress periodically
                if produced % 100 == 0:
                    logger.info(f"Produced {produced} orders so far...")

        except KeyboardInterrupt:
            logger.info(f"Stopping producer after {produced} records")
        finally:
            # Flush any remaining messages
            logger.info("Flushing remaining messages...")
            self.producer.flush(30)
            logger.info("Producer stopped")


def main():
    """Main function to run the Avro order producer"""
    try:
        producer = AvroOrderProducer()
        producer.run_producer()
    except Exception as e:
        logger.error(f"Failed to start producer: {e}")
        raise


if __name__ == "__main__":
    main()
