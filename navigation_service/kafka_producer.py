import json
import logging
from datetime import datetime, timezone
from typing import Optional
from confluent_kafka import Producer
import config

class NavKafkaProducer:
    def __init__(self):
        # TODO 1: Create the configuration dictionary using config.KAFKA_BOOTSTRAP
        conf = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP
        }
        
        # TODO 2: Instantiate the confluent_kafka Producer and assign it to self.producer
        self.producer = Producer(conf)
        self.topic = config.KAFKA_TOPIC_NAV_EVENTS
        logging.info(f"Kafka Producer initialized. Targeting topic: {self.topic}")

    def publish_event(
        self,
        event_type: str,
        distance_cm: Optional[float] = None,
        confidence: Optional[float] = None,
        reaction: Optional[str] = None,
    ) -> None:
        """
        Builds the JSON payload and sends it to Kafka.
        """
        # 1. Build the dictionary payload (Look at Section 7.7 of your guide for the DTO)
        payload = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Only add optional fields if they exist
        if distance_cm is not None:
            payload["distance_cm"] = distance_cm
        if confidence is not None:
            payload["confidence"] = confidence
        if reaction is not None:
            payload["reaction"] = reaction

        # TODO 3: Convert the 'payload' dictionary into a JSON string (use json.dumps)
        json_string = json.dumps(payload)
        
        # TODO 4: Use self.producer to send the JSON string (encoded to utf-8) to self.topic
        self.producer.produce(self.topic, json_string.encode('utf-8'))

        # Poll without blocking so delivery callbacks can progress while the caller stays responsive.
        self.producer.poll(0)

        logging.debug(f"Published to {self.topic}: {payload}")

    def close(self) -> None:
        """Flush queued events before the service shuts down."""
        self.producer.flush()