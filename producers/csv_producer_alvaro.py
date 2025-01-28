import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/producer.log")],
)
logger = logging.getLogger(__name__)


def get_kafka_topic():
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval():
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


def get_data_file_path():
    project_root = pathlib.Path(__file__).resolve().parent.parent
    data_folder = project_root / "data"
    data_file = data_folder / "smoker_temps.csv"
    logger.info(f"Data file: {data_file}")
    return data_file


def create_kafka_producer():
    """Creates a Kafka producer instance."""
    try:
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
        producer = KafkaProducer(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=value_serializer,
        )
        logger.info("Kafka producer created successfully.")
        return producer
    except Exception as e:
        logger.exception(f"Error creating Kafka producer: {e}")
        return None


def generate_messages(file_path: pathlib.Path):
    """Generates JSON messages from the CSV file."""
    while True:
        try:
            with open(file_path, "r") as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    if "temperature" not in row:
                        logger.warning(f"Skipping row missing 'temperature': {row}")
                        continue

                    try:
                        temperature = float(row["temperature"])
                        current_timestamp = datetime.utcnow().isoformat()
                        message = {
                            "timestamp": current_timestamp,
                            "temperature": temperature,
                            "truck_id": row.get("truck_id"),
                            "location": row.get("location"),
                            "engine_status": row.get("engine_status"),
                             "odometer": row.get("odometer")
                        }
                        yield message
                    except ValueError as e:
                        logger.error(f"Error converting temperature: {e}, row: {row}. Skipping.")
                    except Exception as e:
                        logger.error(f"Error generating message: {e}, row: {row}. Skipping.")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Caught general exception: {e}")
            time.sleep(5)  # Wait and retry


def create_kafka_topic(topic_name):
    """Creates or verifies the Kafka topic.  (Placeholder - adapt for your Kafka setup!)"""

    # In a real application, replace this with the logic to create a topic
    # or verify its existence.  For this example it just logs a message and returns True.
    logger.info(f"Topic '{topic_name}' already exists or was created successfully.")
    return True # Signals that the topic was created or already exists


def main():
    logger.info("Starting producer application...")

    topic = get_kafka_topic()
    interval_seconds = get_message_interval()
    data_file_path = get_data_file_path()

    if not data_file_path.exists():
        logger.error(f"Data file not found: {data_file_path}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer()

    if not producer:
        logger.critical("Failed to create producer. Exiting.")
        return


    try:
      create_kafka_topic(topic)
    except Exception as e:
      logger.error(f"Error creating topic '{topic}': {e}. Exiting")
      sys.exit(1)

    try:
        for message in generate_messages(data_file_path):
            producer.send(topic, value=message)
            logger.info(f"Sent message: {message}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted.")
    except Exception as ex:
        logger.error(f"Unhandled exception: {ex}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()



