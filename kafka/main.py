from kafka.kafka import consume_message,produce_message
from db.db import get_db_connection, store_message_in_db
from config import load_config, logger
import threading


def forward_to_local(message, timestamp, kafka_offset):
    """Forward messages from Kafka Server to Kafka Local."""
    logger.info("Forwarding message to Kafka Local...")
    produce_message("kafka_local", "product_view", message)


def save_to_db(message, timestamp, kafka_offset):
    """Save messages from Kafka Local to the database."""
    logger.info("Saving message to database...")
    db_config = load_config("postgresql")
    conn = get_db_connection(db_config)
    store_message_in_db(conn,message, timestamp, kafka_offset)


def consume_from_kafka_server():
    """Consume messages from Kafka Server and forward to Kafka Local."""
    consume_message('kafka_server',["product_view"],forward_to_local)

def consume_from_kafka_local():
    """Consume messages from Kafka Local and store in DB."""
    postgres_handler = consume_message("kafka_local", ["product_view"], save_to_db)
    postgres_handler.run()  # Run indefinitely, consume messages


def main():
    # Create threads for each Kafka consumer
    threading1 = threading.Thread(target=consume_from_kafka_server)
    threading2 = threading.Thread(target=consume_from_kafka_local)

    # Start threads
    threading1.start()
    threading2.start()

    # Wait for threads to finish (though they will run indefinitely)
    threading1.join()
    threading2.join()

if __name__ == "__main__":
    main()
