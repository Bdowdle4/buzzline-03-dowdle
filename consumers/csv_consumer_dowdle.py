"""
csv_consumer_dogs.py

Consume dog data from a Kafka topic and 
count how many times each toy appears.
"""

#####################################
# Import Modules
#####################################

import os
import sys
import json
from collections import Counter

from dotenv import load_dotenv
from kafka import KafkaConsumer

from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("DOG_TOPIC", "dog_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_bootstrap_servers() -> str:
    """Fetch Kafka bootstrap servers from environment or use default."""
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Kafka bootstrap servers: {servers}")
    return servers


#####################################
# Main Consumer Logic
#####################################


def consume_messages():
    """
    Consume messages from the Kafka topic and
    count the frequency of each dog toy.
    """
    topic = get_kafka_topic()
    bootstrap_servers = get_bootstrap_servers()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="dog_consumer_group",
    )

    toy_counter = Counter()
    logger.info("START consumer... Waiting for dog messages...")

    try:
        for message in consumer:
            dog_message = message.value
            logger.info(f"Received: {dog_message}")

            # Count toys
            toy = dog_message.get("toy")
            if toy:
                toy_counter[toy] += 1
                logger.info(f"Updated toy counts: {dict(toy_counter)}")
            else:
                logger.warning(f"No 'toy' field found in message: {dog_message}")

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        sys.exit(1)
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    consume_messages()
