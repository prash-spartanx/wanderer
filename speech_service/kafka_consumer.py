import logging
import json
import threading
from queue import Queue
from confluent_kafka import Consumer, KafkaError, KafkaException
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SpeechKafkaConsumer:
    def __init__(self, speak_callback=None, identify_callback=None):
        self.speak_callback = speak_callback
        self.identify_callback = identify_callback
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._running = False
        self.thread = None
        self._callback_thread = None
        self._command_queue = Queue()
        self.topic = config.KAFKA_TOPIC_BRAIN_COMMANDS
        self.consumer = None

    def start(self):
        with self._state_lock:
            if self.thread is not None and self.thread.is_alive():
                logging.info("Speech Kafka Consumer thread is already running.")
                return

            self._stop_event.clear()
            self._command_queue = Queue()
            self._running = True
            self.thread = threading.Thread(
                target=self._consume_loop,
                name="speech-kafka-consumer",
                daemon=True,
            )
            self._callback_thread = threading.Thread(
                target=self._callback_loop,
                name="speech-command-handler",
                daemon=True,
            )
            self._callback_thread.start()
            self.thread.start()
            logging.info(
                "Speech Kafka Consumer thread started: name=%s, ident=%s, topic=%s",
                self.thread.name,
                self.thread.ident,
                self.topic,
            )
        
    def stop(self):
        with self._state_lock:
            thread = self.thread
            if thread is None:
                return
            self._stop_event.set()

        thread.join(timeout=3)
        if thread.is_alive():
            logging.error(
                "Speech Kafka Consumer thread did not stop within the timeout."
            )
        else:
            logging.info("Speech Kafka Consumer thread stopped.")

        with self._state_lock:
            self._running = False
            self.thread = None
            self._command_queue.put(None)

        callback_thread = self._callback_thread
        if callback_thread is not None:
            callback_thread.join(timeout=3)
            if callback_thread.is_alive():
                logging.error("Speech command handler did not stop within the timeout.")
            self._callback_thread = None

    @property
    def is_running(self) -> bool:
        thread = self.thread
        return self._running and thread is not None and thread.is_alive()

    @property
    def handler_thread_name(self) -> str | None:
        return self._callback_thread.name if self._callback_thread else None

    @property
    def is_handler_running(self) -> bool:
        return (
            self._callback_thread is not None
            and self._callback_thread.is_alive()
        )

    def _consume_loop(self):
        conf = {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
        consumer = None

        try:
            # Keep all Kafka client operations on this thread.
            consumer = Consumer(conf)
            self.consumer = consumer
            consumer.subscribe(
                [self.topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            logging.info(
                "Speech Kafka consumer listening on topic: %s (thread=%s, ident=%s)",
                self.topic,
                threading.current_thread().name,
                threading.get_ident(),
            )

            while not self._stop_event.is_set():
                try:
                    msg = consumer.poll(timeout=1.0)
                except KafkaException:
                    logging.exception("Kafka poll failed; consumer loop will retry.")
                    continue

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error("Consumer error: %s", msg.error())
                    continue

                try:
                    message_str = msg.value().decode("utf-8")
                    command_data = json.loads(message_str)
                    if not isinstance(command_data, dict):
                        raise ValueError("Kafka command must be a JSON object")

                    logging.info("Received brain command: %s", command_data)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    logging.exception("Invalid brain command message; ignoring it.")
                    continue

                target = command_data.get("target")

                if target != "speech":
                    logging.debug("Ignoring command for target: %s", target)
                    continue

                command = str(command_data.get("command", "")).lower()

                self._command_queue.put((command, command_data))
        except KafkaException:
            logging.exception("Speech Kafka consumer could not start.")
        except Exception:
            logging.exception("Unexpected failure in speech Kafka consumer loop.")
        finally:
            if consumer is not None:
                consumer.close()
            self.consumer = None
            self._running = False
            logging.info("Speech Kafka consumer loop exited.")

    def _callback_loop(self):
        while True:
            try:
                item = self._command_queue.get()
            except Exception:
                logging.exception("Speech command queue failed.")
                return

            if item is None:
                return

            command, command_data = item
            try:
                if command == "speak":
                    text_to_say = command_data.get("text", "")
                    if self.speak_callback:
                        logging.info("Executing speak command: %s", text_to_say)
                        self.speak_callback(str(text_to_say))
                    else:
                        logging.warning("No speak_callback provided to handle speak command.")
                elif command == "identify":
                    identify_text = command_data.get("text", "")
                    if self.identify_callback:
                        logging.info("Executing identify command: %s", identify_text)
                        self.identify_callback(str(identify_text))
                    else:
                        logging.warning("No identify_callback provided to handle identify command.")
                else:
                    logging.warning("Unhandled speech command: %s", command)
            except Exception:
                logging.exception(
                    "Brain command callback failed; keeping Kafka consumer alive."
                )
            finally:
                self._command_queue.task_done()

    @staticmethod
    def _on_assign(consumer, partitions):
        logging.info("Speech Kafka consumer assigned %d partition(s).", len(partitions))

    @staticmethod
    def _on_revoke(consumer, partitions):
        logging.warning("Speech Kafka consumer partitions revoked: %d", len(partitions))
