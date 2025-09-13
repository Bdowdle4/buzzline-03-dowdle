"""
json_consumer_dowdle.py

Consume JSON messages about dogs from a Kafka topic and process them.

Example serialized Kafka message
"{\"name\": \"Bubba\", \"breed\": \"Golden Retriever\", \"age\": 3, \"favorite_toy\": \"Tennis ball\"}"

Example JSON message (after deserialization)
{"name": "Bubba", "breed": "Golden Retriever", "age": 3, "favorite_toy": "Tennis ball"}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict  # used to track toy counts

from dotenv import load_dotenv

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("DOG_TOPIC", "dogs_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("DOG_CONSUMER_GROUP_ID", "dog_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Global Data Store: Toy Counts
#####################################

# {toy: count}
toy_counts: defaultdict[str, int] = defaultdict(int)


#####################################
# Function to process a single dog message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON dog message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        from typing import Any
        message_dict: dict[str, Any] = json.loads(message)

        logger.info(f"Processed Dog JSON: {message_dict}")

        # Extract fields from the dog message
        name = message_dict.get("name", "Unknown")
        breed = message_dict.get("breed", "Unknown")
        age = message_dict.get("age", "Unknown")
        toy = message_dict.get("favorite_toy", "Unknown")

        # Update toy counts
        toy_counts[toy] += 1

        # Log nicely formatted info
        logger.info(
            f"Dog '{name}' is a {age}-year-old {breed} who loves '{toy}'."
        )
        logger.info(f"Updated toy counts: {dict(toy_counts)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    """
    Main entry point for the dog consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes dog messages from the Kafka topic and tracks toy counts.
    """
    logger.info("START dog consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling dog messages from topic '{topic}'...")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    message_str: str = msg.value
                    logger.debug(
                        f"Received message at offset {msg.offset}: {message_str}"
                    )
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()

    logger.info(f"Kafka consumer for topic '{topic}' closed.")
    logger.info(f"Final toy counts: {dict(toy_counts)}")
    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")
    

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
