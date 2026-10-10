import json
import logging 
from datetime import datetime, timezone
from confluent_kafka import Producer
import config

logging.basicConfig(level=logging.INFO)

class VisionKafkaProducer:
    def __init__(self):
        conf = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP,   
        }
        self.producer = Producer(conf)
        # set target topic for the producer
        self.topic = config.KAFKA_TOPIC_VISION_EVENTS

    def publish_event(
        self,
        event: str,
        name: str = None,
        confidence: float = None,
        object_name: str = None,
    ) -> bool:
        payload = {
            "event": event,
            "name": name,
            "confidence": confidence,
            "objectName": object_name,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
            
        try:
            self.producer.produce(
                self.topic,
                json.dumps(payload).encode("utf-8"),
                on_delivery=self._delivery_report,
            )
            remaining = self.producer.flush(timeout=5.0)
            if remaining:
                logging.error(
                    "Kafka publish timed out with %d message(s) still queued.", remaining
                )
                return False
            return True
        except Exception:
            logging.exception("Failed to publish event to Kafka.")
            return False

    @staticmethod
    def _delivery_report(err, message):
        if err is not None:
            logging.error("Kafka delivery failed for topic %s: %s", message.topic(), err)