import json
import logging
import threading
from confluent_kafka import Consumer, KafkaError
import config


class NavKafkaConsumer:
    def __init__(self, wander_engine):
        """
        Initialize the Kafka Consumer to listen for brain commands.
        """
        self.wander_engine = wander_engine
        self.running = False
        self.thread = None
        
        # Configure the consumer
        conf = {
            'bootstrap.servers': config.KAFKA_BOOTSTRAP,
            'group.id': config.KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        
        self.consumer = Consumer(conf)
        self.topic = config.KAFKA_TOPIC_BRAIN_COMMANDS
        self.consumer.subscribe([self.topic])
        logging.info(f"Kafka Consumer initialized. Listening to topic: {self.topic}")

    def start(self):
        """Spin up the background consumer thread."""
        if self.thread is None or not self.thread.is_alive():
            self.running = True
            self.thread = threading.Thread(target=self._consume_loop, daemon=True)
            self.thread.start()
            logging.info("Kafka Consumer thread started.")

    def stop(self):
        """Signal the consumer thread to shut down."""
        self.running = False
        if self.thread:
            self.thread.join()
        if self.consumer:
            self.consumer.close()
        logging.info("Kafka Consumer thread stopped.")

    def _consume_loop(self):
        """The continuous background loop that listens for messages."""
        while self.running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

            try:
                
                
              
                # 1. Decode the message value from bytes to string
                message_str = msg.value().decode('utf-8')
                
                # 2. Parse the JSON string into a dictionary
                command_obj = json.loads(message_str)
                target = command_obj.get('target')
                if target != "navigation":
                    logging.debug(
                        "Ignoring command for target '%s'; this consumer handles "
                        "navigation only.",
                        target,
                    )
                    continue

                command = command_obj.get("command")
                if not isinstance(command, str):
                    logging.warning("Navigation command is missing or not a string.")
                    continue

                normalized_command = command.strip().upper()
                logging.info(
                    "Received navigation command: %s (text=%r)",
                    normalized_command,
                    command_obj.get("text"),
                )

                # Brain uses START/STOP for navigation lifecycle commands.
                if normalized_command in {"START", "START_WANDER"}:
                    logging.info("Starting the navigation engine...")
                    self.wander_engine.start()
                    logging.info(
                        "Navigation engine started; state=%s",
                        self.wander_engine.state,
                    )

                elif normalized_command in {"STOP", "PAUSE_WANDER"}:
                    logging.info("Stopping the navigation engine...")
                    self.wander_engine.pause()
                    logging.info(
                        "Navigation engine stopped; state=%s",
                        self.wander_engine.state,
                    )

                elif normalized_command == "RESUME_WANDER":
                    logging.info("Resuming the navigation engine...")
                    self.wander_engine.resume()
                    logging.info(
                        "Navigation engine resumed; state=%s",
                        self.wander_engine.state,
                    )

                elif normalized_command == "ROTATE_LEFT":
                    logging.info("Executing left rotation...")
                    self.wander_engine.execute_manual_turn("LEFT")
                    
                elif normalized_command == "ROTATE_RIGHT":
                    logging.info("Executing right rotation...")
                    self.wander_engine.execute_manual_turn("RIGHT")
                    
                else:
                    logging.warning(f"Unknown navigation command received: {command}")
                    
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON message: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
