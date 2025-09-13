"""
json_producer_dowdle.py

Stream JSON data about dogs to a Kafka topic.

Example JSON message
{"name": "Bubba", "breed": "Golden Retriever", "age": 3, "favorite_toy": "Tennis ball"}

Example serialized to Kafka message
"{\"name\": \"Bubba\", \"breed\": \"Golden Retriever\", \"age\": 3, \"favorite_toy\": \"Tennis ball\"}"
"""

#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import json
import random
from typing import Generator, Dict, Any

from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    topic = os.getenv("DOG_TOPIC", "dogs_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    interval = int(os.getenv("DOG_INTERVAL_SECONDS", 4))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################


DOG_NAMES = ["Nimeria", "Duke", "Rex", "Louie", "Bubba", "Daisy", "Harley", "Bailey"]
DOG_BREEDS = [
    "Golden Retriever",
    "Labrador",
    "German Shepherd",
    "Poodle",
    "Bulldog",
    "Beagle",
]
DOG_TOYS = ["Tennis ball", "Rope", "Squeaky toy", "Frisbee", "Stick", "Stuffie"]


def generate_dog_messages() -> Generator[Dict[str, Any], None, None]:
    """
    Continuously generate JSON messages about dogs.
    """
    while True:
        message = {
            "name": random.choice(DOG_NAMES),
            "breed": random.choice(DOG_BREEDS),
            "age": random.randint(1, 15),
            "favorite_toy": random.choice(DOG_TOYS),
        }
        logger.debug(f"Generated dog JSON: {message}")
        yield message


#####################################
# Main Function
#####################################


def main():
    logger.info("START dog producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting dog message production to topic '{topic}'...")
    try:
        for message_dict in generate_dog_messages():
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close(timeout=None)
        logger.info("Kafka producer closed.")

    logger.info("END dog producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
