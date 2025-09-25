import psycopg2
import logging
from config.app_config import load_config

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load database configuration

def get_db_connection(db_config):
    try:
        conn = psycopg2.connect(
            dbname = db_config['dbname'],
            user = db_config['user'],
            password = db_config['password'],
            host = db_config['host'],
            port = db_config['port']

        )
        logger.info("Connected to database successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def store_message_in_db(conn,message,timestamp,kafka_offset):
    """Store Kafka messages in the PostgreSQL database."""
    cursor = conn.cursor()

    try:
        if not isinstance(message, str):
            message = message.decode('utf-8')
        # Use ON CONFLICT to avoid duplicate messages based on the kafka_offset
        cursor.execute("""
            INSERT INTO kafka_data (message, timestamp, kafka_offset)
            VALUES (%s, to_timestamp(%s), %s)
            ON CONFLICT (kafka_offset) DO NOTHING
        """, (message, timestamp, kafka_offset))
        conn.commit()
        logger.info(f"Message stored successfully: Offset={kafka_offset}, Message={message}")

    except Exception as e:
        logger.error(f"Failed to store message: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


