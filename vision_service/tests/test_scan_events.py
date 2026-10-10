import json
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def install_import_stubs():
    """Keep these tests runnable without camera, Kafka, or ML packages."""
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    sys.modules["dotenv"] = dotenv

    fastapi = types.ModuleType("fastapi")

    class FastAPI:
        def __init__(self, **kwargs):
            pass

        def get(self, path):
            return lambda function: function

        def post(self, path):
            return lambda function: function

    fastapi.FastAPI = FastAPI
    sys.modules["fastapi"] = fastapi

    uvicorn = types.ModuleType("uvicorn")
    uvicorn.run = lambda *args, **kwargs: None
    sys.modules["uvicorn"] = uvicorn

    camera_controller = types.ModuleType("camera.camera_controller")

    class CameraController:
        def __init__(self, *args, **kwargs):
            pass

    camera_controller.CameraController = CameraController
    sys.modules["camera.camera_controller"] = camera_controller

    embeddings_store = types.ModuleType("detection.embeddings_store")

    class EmbeddingsStore:
        def __init__(self, *args, **kwargs):
            pass

    embeddings_store.EmbeddingsStore = EmbeddingsStore
    sys.modules["detection.embeddings_store"] = embeddings_store

    face_detector = types.ModuleType("detection.face_detector")

    class FaceDetector:
        def __init__(self, *args, **kwargs):
            pass

    face_detector.FaceDetector = FaceDetector
    sys.modules["detection.face_detector"] = face_detector

    object_detector = types.ModuleType("detection.object_detector")

    class ObjectDetector:
        def __init__(self, *args, **kwargs):
            pass

    object_detector.ObjectDetector = ObjectDetector
    sys.modules["detection.object_detector"] = object_detector

    kafka_producer = types.ModuleType("kafka_producer")

    class VisionKafkaProducer:
        def __init__(self, *args, **kwargs):
            pass

    kafka_producer.VisionKafkaProducer = VisionKafkaProducer
    sys.modules["kafka_producer"] = kafka_producer

    confluent_kafka = types.ModuleType("confluent_kafka")

    class Consumer:
        def __init__(self, config):
            pass

        def subscribe(self, topics, **kwargs):
            pass

        def close(self):
            pass

    confluent_kafka.Consumer = Consumer
    confluent_kafka.KafkaError = Mock()
    confluent_kafka.KafkaException = Exception
    sys.modules["confluent_kafka"] = confluent_kafka


install_import_stubs()
import main


class FakeKafkaMessage:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def value(self):
        return self.payload

    def error(self):
        return None

    def topic(self):
        return "brain-commands"

    def partition(self):
        return 0

    def offset(self):
        return 0


class OneScanConsumer:
    """Returns one Brain command, then stops the consumer loop."""

    def __init__(self, message):
        self.message = message
        self.owner = None
        self.poll_count = 0

    def poll(self, timeout):
        self.poll_count += 1
        if self.poll_count == 1:
            return self.message

        self.owner.running = False
        return None


class ScanEventTests(unittest.TestCase):
    def setUp(self):
        self.frame = object()
        self.camera = Mock()
        self.camera.get_frame.return_value = self.frame
        self.producer = Mock()

        main.camera_controller = self.camera
        main.kafka_producer = self.producer
        main.kafka_consumer = main.create_kafka_consumer()

    def send_scan_command(self, face_results, object_result=None):
        main.face_detector = Mock()
        main.face_detector.process_frame.return_value = face_results
        main.object_detector = Mock()
        main.object_detector.process_frame.return_value = object_result

        consumer = OneScanConsumer(
            FakeKafkaMessage({"target": "vision", "command": "scan"})
        )
        consumer.owner = main.kafka_consumer
        main.kafka_consumer.consumer = consumer
        main.kafka_consumer.running = True
        main.kafka_consumer._consume_loop()
        self.camera.get_frame.assert_called_once_with()

    def test_unknown_person_event_is_published_after_scan_command(self):
        self.send_scan_command(
            [{"name": "unknown", "confidence": 0.82, "embedding": object()}]
        )

        self.producer.publish_event.assert_called_once_with(
            event="unknown_person_detected", confidence=0.82
        )

    def test_known_person_event_contains_person_text(self):
        self.send_scan_command(
            [{"name": "Alice", "confidence": 0.96, "embedding": object()}]
        )

        self.producer.publish_event.assert_called_once_with(
            event="person_detected", name="Alice", confidence=0.96
        )

    def test_object_event_contains_object_text_when_no_face_is_found(self):
        self.send_scan_command(
            [],
            object_result={"objectName": "cup", "confidence": 0.91},
        )

        self.producer.publish_event.assert_called_once_with(
            event="object_detected",
            object_name="cup",
            confidence=0.91,
        )

    def test_no_face_event_is_published_when_no_face_or_object_is_found(self):
        self.send_scan_command([], object_result=None)

        self.producer.publish_event.assert_called_once_with(
            event="no_face_detected"
        )

    def test_no_face_detected_event_is_published_when_scene_is_empty(self):
        self.send_scan_command([], object_result=None)

        self.producer.publish_event.assert_called_once_with(
            event="no_face_detected"
        )


class RealCameraScanTests(unittest.TestCase):
    """Hardware/inference check; run explicitly with RUN_CAMERA_TESTS=1."""

    @unittest.skipUnless(
        os.getenv("RUN_CAMERA_TESTS") == "1",
        "Set RUN_CAMERA_TESTS=1 to run the real camera integration test.",
    )
    def test_camera_frame_is_checked_for_faces_and_objects(self):
        for module_name in (
            "camera.camera_controller",
            "detection.embeddings_store",
            "detection.face_detector",
            "detection.object_detector",
        ):
            sys.modules.pop(module_name, None)

        try:
            from camera.camera_controller import CameraController
            from detection.embeddings_store import EmbeddingsStore
            from detection.face_detector import FaceDetector
            from detection.object_detector import ObjectDetector
        except ImportError as exc:
            self.skipTest(f"Vision dependency is not installed: {exc}")
        except SystemExit:
            self.skipTest(
                "face_recognition_models is missing or incomplete. "
                "Install it before running the real camera test."
            )

        camera_index = int(os.getenv("CAMERA_INDEX", "0"))
        database_path = os.getenv("KNOWN_FACES_DB", "known_faces.pkl")
        camera = CameraController(camera_index)
        producer = Mock()
        store = EmbeddingsStore(database_path)
        known_faces = store.load_known_faces()
        face_detector = FaceDetector(store)
        object_detector = ObjectDetector()

        main.camera_controller = camera
        main.embeddings_store = store
        main.face_detector = face_detector
        main.object_detector = object_detector
        main.kafka_producer = producer

        try:
            consumer = OneScanConsumer(
                FakeKafkaMessage({"target": "vision", "command": "scan"})
            )
            consumer.owner = main.kafka_consumer
            main.kafka_consumer.consumer = consumer
            main.kafka_consumer.running = True

            # This is the only command sent to the production scan callback.
            main.kafka_consumer._consume_loop()

            if camera.cap is None or not camera.cap.isOpened():
                self.skipTest(
                    f"Camera {camera_index} could not be opened by the scan command. "
                    "Check camera permissions and connection."
                )

            self.assertEqual(consumer.poll_count, 2)
            self.assertTrue(producer.publish_event.called, "No scan event was published")
            events = [call.kwargs["event"] for call in producer.publish_event.call_args_list]
            self.assertTrue(
                set(events).issubset(
                    {
                        "person_detected",
                        "unknown_person_detected",
                        "object_detected",
                        "no_face_detected",
                    }
                ),
                f"Unexpected scan events: {events}",
            )

            for call in producer.publish_event.call_args_list:
                if call.kwargs["event"] == "person_detected":
                    self.assertIn(call.kwargs["text"], known_faces)
                elif call.kwargs["event"] == "object_detected":
                    self.assertIsInstance(call.kwargs["text"], str)

            print(
                f"One scan command completed: events={events}; "
                f"database={database_path} ({len(known_faces)} entries)"
            )
        finally:
            camera.release()


if __name__ == "__main__":
    unittest.main()