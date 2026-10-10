import json
import logging
from confluent_kafka import Producer
from datetime import datetime, timezone
import config 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SpeechKafkaProducer:
    def __init__(self):
        conf = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        }
        self.producer = Producer(conf)
        
        # Load BOTH topics from config
        self.events_topic = config.KAFKA_TOPIC_SPEECH_EVENTS
        self.completions_topic = getattr(config, 'KAFKA_TOPIC_TASK_COMPLETIONS', 'task-completions')
        logging.info(f"Speech Kafka Producer initialized. Targets: {self.events_topic}, {self.completions_topic}")

    def publish_event(self, event: str, text: str = None) -> None:
        """Publishes what the robot hears (wake word, transcribed text)"""
        payload = {
            "event": event,
            "text": text or "",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        try:
            # Notice we pass self.events_topic here
            self.producer.produce(self.events_topic, json.dumps(payload).encode('utf-8'))
            self.producer.flush()
            logging.info(f"Published event to {self.events_topic}: {payload}")
        except Exception as e:
            logging.error("Failed to publish event to Kafka: %s", e)

    def publish_task_completion(self, task_name: str = "speak", status: str = "success") -> None:
        """Publishes that a Brain command (like speaking) has finished"""
        payload = {
            "task": task_name,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Notice we pass self.completions_topic here
            self.producer.produce(self.completions_topic, json.dumps(payload).encode('utf-8'))
            self.producer.flush()
            logging.info(f"Published completion to {self.completions_topic}: {payload}")
        except Exception as e:
            logging.error("Failed to publish task completion: %s", e)