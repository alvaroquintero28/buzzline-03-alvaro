import json
import logging
from kafka import KafkaConsumer
import polars as pl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/consumer.log")],
)
logger = logging.getLogger(__name__)


def get_kafka_topic():
    topic = "truck_telemetry" # Set to the topic in the producer script
    # topic = os.environ.get("BUZZ_TOPIC", "truck_telemetry") # If using environment variables
    logger.info(f"Kafka topic: {topic}")
    return topic


def create_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092",  # Replace with your Kafka servers
            auto_offset_reset="earliest",  # Or "latest" to start from the latest messages
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),   # Important: decode & deserialize
        )
        consumer.subscribe([get_kafka_topic()])  # Subscribe to the topic
        return consumer
    except Exception as e:
        logger.exception(f"Error creating Kafka consumer: {e}. Exiting.")
        return None


def process_messages(consumer):
    try:
        for message in consumer:
            try:
                telemetry_data = message.value
                logger.info(f"Received message: {telemetry_data}")

                # Data processing logic that uses polars for efficient DataFrame creation:
                df = pl.DataFrame(telemetry_data)  #  Creates a DataFrame

                #  More robust processing and handling different data structures: 
                if df.shape[0] > 0: # Ensure data is present in the DataFrame before attempting calculations
                    # e.g.:
                    avg_odometer = df.select(pl.col("odometer").mean()).to_series().to_list()[0]  # Use to_series to get single value
                    logger.info(f"Average odometer: {avg_odometer}")
                    # ... other calculations
                    # ... write data to database/another destination
                    # Write the DataFrame to a CSV file
                    df.write_csv("processed_telemetry_data.csv") # Example: CSV file output
                else:
                    logger.warning("Received message with empty DataFrame. Skipping")


            except (KeyError, TypeError, json.JSONDecodeError) as e:
                logger.error(f"Error processing message: {e}, Incorrect Message Format: {message}, Skipping.")

            except Exception as e:  # Catch other possible exceptions
                logger.error(f"An unexpected error occurred: {e}. Skipping.")
    except KeyboardInterrupt:  # Handle Ctrl+C
        consumer.close()
        logger.info("Consumer stopped gracefully.")


    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}. Exiting.")


if __name__ == "__main__":
    consumer = create_kafka_consumer()

    if consumer:
        process_messages(consumer)


