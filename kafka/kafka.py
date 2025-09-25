from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from config import load_config, logger
import json

def consume_message(consumer_info,topic,callback):
    """Consume messages from Kafka and process them using a callback."""
    consumer_config = load_config(consumer_info)
    consumer = Consumer(consumer_config)
    consumer.subscribe(topic)
    logger.info("Kafka Consumer initialized successfully.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition.")
                    continue
                else:
                    raise KafkaException(msg.error())

            # Log and process the message
            logger.info("Consumed message: topic=%s, key=%s, value=%s", msg.topic(), msg.key(), msg.value())
            callback(msg.value(), msg.timestamp()[1], msg.offset())

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()
        logger.info("Kafka Consumer closed.")


def delivery_report(err,msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def produce_message(producer_info,topic,message):
    """Produce messages to Kafka."""
    producer_config = load_config(producer_info)
    producer = Producer(producer_config)

    # Serialize message as JSON
    if isinstance(message,dict):
        message = json.dump(message)

    try:
        producer.produce(topic, message, callback=delivery_report)
        producer.flush()
        logger.info("Message produced successfully.")
        logger.info("Kafka Producer closed.")
    except Exception as e:
        logger.error("Failed to produce message: %s", e)

