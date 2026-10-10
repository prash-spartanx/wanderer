import json
import logging
import queue
import threading
from typing import Any, Callable, Optional

import config
from confluent_kafka import Consumer, KafkaError, KafkaException


class VisionKafkaConsumer:
    def __init__(self, scan_callback=None, save_face_callback=None):
        self.scan_callback = scan_callback
        self.save_face_callback = save_face_callback
        self.running = False
        self.thread = None
        self.command_queue: queue.Queue = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None
        self.scan_state_lock = threading.Lock()
        self.scan_active = False
        self.assignment_logged = False
        self.closed = False

        conf = {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
        logging.info(
            "Configuring Kafka command consumer: broker=%s topic=%s group=%s",
            config.KAFKA_BOOTSTRAP,
            config.KAFKA_TOPIC_BRAIN_COMMANDS,
            config.KAFKA_GROUP_ID,
        )
        self.consumer = Consumer(conf)
        self.topic = config.KAFKA_TOPIC_BRAIN_COMMANDS
        self.consumer.subscribe([self.topic], on_assign=self._on_assign)
        logging.info("Kafka command consumer subscribed to topic '%s'.", self.topic)

    def _on_assign(self, consumer, partitions):
        assignment = [
            (partition.topic, partition.partition, partition.offset)
            for partition in partitions
        ]
        logging.info(
            "Kafka command consumer assigned %d partition(s): %s",
            len(assignment),
            assignment,
        )
        self.assignment_logged = True

    def start(self):
        """Start the consumer loop in a separate thread."""
        if self.running:
            return

        self.running = True
        self.worker_thread = threading.Thread(
            target=self._command_loop,
            name="vision-command-worker",
            daemon=True,
        )
        self.worker_thread.start()
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()
        logging.info(
            "Vision Kafka Consumer thread started (alive=%s).",
            self.thread.is_alive(),
        )

    def stop(self):
        """Stop the consumer loop and close the consumer."""
        if self.closed:
            return
        self.running = False
        if self.thread is not None:
            self.thread.join(timeout=2)
        self.command_queue.put(None)
        if self.worker_thread is not None:
            self.worker_thread.join(timeout=5)
            if self.worker_thread.is_alive():
                logging.error(
                    "Vision command worker did not stop within 5 seconds; "
                    "a callback may still be blocked."
                )
        if self.consumer is not None:
            self.consumer.close()
        self.closed = True
        logging.info("Vision Kafka Consumer thread stopped.")

    def _command_loop(self):
        while True:
            work = self.command_queue.get()
            try:
                if work is None:
                    return
                callback, args = work
                callback(*args)
            except Exception:
                logging.exception("Unhandled error while processing vision command.")
            finally:
                self.command_queue.task_done()

    def _submit(self, callback: Optional[Callable[..., Any]], *args):
        if callback is None:
            return
        if self.worker_thread is None or not self.worker_thread.is_alive():
            # Keeps direct loop tests and controlled callers deterministic.
            callback(*args)
            return
        self.command_queue.put((callback, args))

    def _submit_scan(self):
        with self.scan_state_lock:
            if self.scan_active:
                logging.warning("Scan already in progress; ignoring duplicate scan command.")
                return
            self.scan_active = True

        def run_scan():
            try:
                if self.scan_callback is not None:
                    self.scan_callback()
            finally:
                with self.scan_state_lock:
                    self.scan_active = False

        if self.worker_thread is None or not self.worker_thread.is_alive():
            run_scan()
        else:
            self.command_queue.put((run_scan, ()))

    def _consume_loop(self):
        """Continuously poll for messages and process them."""
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if not self.assignment_logged:
                        assignments = (
                            self.consumer.assignment()
                            if hasattr(self.consumer, "assignment")
                            else []
                        )
                        if assignments:
                            assignment = [
                                (
                                    partition.topic,
                                    partition.partition,
                                    partition.offset,
                                )
                                for partition in assignments
                            ]
                            logging.info(
                                "Kafka command consumer assigned %d partition(s): %s",
                                len(assignment),
                                assignment,
                            )
                            self.assignment_logged = True
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue

                    logging.error("Consumer error: %s", msg.error())
                    continue

                try:
                    message_str = msg.value().decode("utf-8")
                    logging.info(
                        "Received Kafka command from %s[%s]@%s: %s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        message_str,
                    )
                    command_obj = json.loads(message_str)
                except Exception as exc:
                    logging.error("Failed to parse incoming Kafka message: %s", exc)
                    continue

                target = command_obj.get("target")
                if target != "vision":
                    logging.warning(
                        "Received command for target '%s', but this consumer only handles 'vision'. Ignoring.",
                        target,
                    )
                    continue

                command = command_obj.get("command")
                normalized_command = command.lower() if isinstance(command, str) else command
                if normalized_command in {"scan", "manual_scan", "scan/manual"}:
                    self._submit_scan()
                    continue

                if normalized_command in {"saveface", "save_face"}:
                    face_name = command_obj.get("text") or command_obj.get("name") or command_obj.get("value")
                    if not face_name:
                        logging.warning("saveface command received without a name field.")
                        continue

                    self._submit(self.save_face_callback, face_name)
                    continue

                logging.warning("Unsupported vision command: %s", command)
        except KafkaException:
            logging.exception("Kafka command consumer stopped because of a Kafka error.")
        except Exception:
            logging.exception(
                "Kafka command consumer stopped because of an unexpected error."
            )
        finally:
            self.running = False
            logging.error("Kafka command polling loop exited.")

    def status(self) -> dict[str, object]:
        return {
            "running": self.running,
            "poll_thread_alive": self.thread is not None and self.thread.is_alive(),
            "worker_thread_alive": (
                self.worker_thread is not None and self.worker_thread.is_alive()
            ),
            "topic": self.topic,
            "group": config.KAFKA_GROUP_ID,
        }
                