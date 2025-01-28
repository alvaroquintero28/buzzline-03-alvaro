import os
import sys
import time
import json
from datetime import datetime
from kafka import KafkaConsumer
import logging

# Configure logging (same as producer)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/consumer.log")],
)
logger = logging.getLogger(__name__)


def get_kafka_topic():
    return os.getenv("SMOKER_TOPIC", "unknown_topic")


def create_kafka_consumer():
    """Creates a Kafka consumer instance."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            auto_offset_reset="earliest",  # Start from the beginning
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )
        consumer.subscribe([get_kafka_topic()])
        return consumer
    except Exception as e:
        logger.exception(f"Error creating Kafka consumer: {e}")
        return None


def consume_messages(consumer):
    """Consumes and processes messages from the Kafka topic."""
    try:
        for message in consumer:
            try:
                data = message.value
                logger.info(f"Received message: {data}")

                # Data processing logic (e.g., storing in a database)
                timestamp = data.get('timestamp')
                temperature = data.get('temperature')

                if timestamp and temperature:
                    logger.info(f"Temperature: {temperature} at {timestamp}")
                else:
                    logger.warning("Incomplete message received.")
            except (KeyError, json.JSONDecodeError) as e:
                logger.error(f"Error processing message: {e}, message: {message}. Skipping.")  # Handle incomplete data
            except Exception as e:
                logger.error(f"Unexpected error: {e}, message: {message}. Skipping.")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted.")
    except Exception as ex:  # Catch other exceptions
        logger.error(f"Unhandled exception: {ex}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    consumer = create_kafka_consumer()

    if consumer:
        consume_messages(consumer)
