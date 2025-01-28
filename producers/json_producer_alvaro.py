import os
import sys
import time
import pathlib
import json
from kafka import KafkaProducer
import logging
import polars as pl

# Configure logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("logs/producer.log")],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


def get_kafka_topic():
    topic = os.environ.get("BUZZ_TOPIC", "truck_telemetry")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval():
    interval = int(os.environ.get("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


def get_data_file():
    project_root = pathlib.Path(__file__).parent.parent
    data_folder = project_root / "data"
    data_file = data_folder / "buzz.json"
    logger.info(f"Data file: {data_file}")
    return data_file


def generate_messages(file_path: pathlib.Path):
    try:
        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                logger.critical(f"Invalid JSON format in {file_path}: {e}. Exiting.")
                return  # Return early on JSON decoding error

        if not isinstance(data, list):
            logger.critical(f"Data in {file_path} is not a valid JSON array. Exiting.")
            return  # Return early if not a list


        for row_dict in data:  # Iterate through JSON objects
            try:
                message_dict = {
                    "type": "truck_data",
                    "truck_id": row_dict.get("truck_id", None),
                    "odometer": row_dict.get("odometer", None),
                    "location": row_dict.get("location", None),
                    "engine_status": row_dict.get("engine_status", None),
                    "timestamp": row_dict.get("timestamp", None),
                }
                yield message_dict

            except (KeyError, TypeError) as e:
                logger.error(f"Error processing row: {e}, Row Data: {row_dict}. Skipping.")
                continue  # Skip to the next row on error/incomplete data

    except FileNotFoundError as e:
        logger.critical(f"Error: {e}. Exiting.")
        raise
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}. Exiting.")
        raise


def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        return producer
    except Exception as e:
        logger.exception(f"Error creating Kafka producer: {e}. Exiting.")
        return None


def create_kafka_topic(topic_name):
    # Placeholder – implement actual topic creation logic for production
    logger.info(f"Topic '{topic_name}' already exists.")
    return True



def main():
    logger.info("Starting producer application...")
    try:
        topic = get_kafka_topic()
        interval_seconds = get_message_interval()
        data_file = get_data_file()
        producer = create_kafka_producer()

        if not producer:
            logger.critical("Failed to create producer. Exiting.")
            return

        create_kafka_topic(topic)

        for message in generate_messages(data_file):
            try:
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
            except Exception as e:
                logger.error(f"Error sending message: {e}. Skipping message.")
                continue
            time.sleep(interval_seconds)

    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}. Exiting.")
    finally:
        if producer:
            producer.flush()
            producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()



