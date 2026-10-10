import logging
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
import uvicorn
import config

# Local imports
from camera.camera_controller import CameraController
from detection.embeddings_store import EmbeddingsStore
from detection.face_detector import FaceDetector
from detection.object_detector import ObjectDetector
from kafka_producer import VisionKafkaProducer
from kafka_consumer import VisionKafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. Global Instances ---
camera_controller = CameraController(config.CAMERA_INDEX)
embeddings_store = EmbeddingsStore()
face_detector = FaceDetector(embeddings_store)
object_detector = ObjectDetector()
kafka_producer = VisionKafkaProducer()
embedding_lock = threading.Lock()
scan_lock = threading.Lock()
last_unknown_embedding = None

# --- 2. The Callback (Fixed Logic) ---
def scan_callback():
    global last_unknown_embedding
    logging.info("Executing manual scan sequence...")
    scan_started = time.monotonic()
    try:
        with scan_lock:
            logging.info("Capturing camera frame...")
            frame = camera_controller.get_frame()
            if frame is None:
                logging.error("No frame captured from camera.")
                return

            logging.info("Running face detection...")
            face_results = face_detector.process_frame(frame)

            if face_results:
                for result in face_results:
                    face_embedding = result.get("embedding")
                    if result["name"] == "unknown":
                        with embedding_lock:
                            last_unknown_embedding = face_embedding
                        kafka_producer.publish_event(
                            event="unknown_person_detected",
                            confidence=result["confidence"],
                        )
                    else:
                        kafka_producer.publish_event(
                            event="person_detected",
                            name=result["name"],
                            confidence=result["confidence"],
                        )
            else:
                logging.info("No face found; running object detection...")
                object_result = object_detector.process_frame(frame)
                if object_result:
                    kafka_producer.publish_event(
                        event="object_detected",
                        object_name=object_result["objectName"],
                        confidence=object_result["confidence"],
                    )
                else:
                    kafka_producer.publish_event(event="no_face_detected")
    except Exception:
        logging.exception("Error during scan callback.")
    finally:
        with scan_lock:
            camera_controller.release()
        logging.info(
            "Scan sequence finished in %.2f seconds.",
            time.monotonic() - scan_started,
        )


def save_face_callback(face_name: str):
    global last_unknown_embedding
    logging.info("Received saveface command for '%s'.", face_name)

    with embedding_lock:
        embedding_to_save = last_unknown_embedding

    if embedding_to_save is None:
        logging.warning("No unknown face embedding available to save for '%s'.", face_name)
        return

    try:
        if embeddings_store.save_new_face(face_name, embedding_to_save):
            kafka_producer.publish_event(event="saved_face", name=face_name)
        else:
            logging.error(
                "Failed to save face '%s' from the last unknown embedding.", face_name
            )
    except Exception:
        logging.exception("Error while saving face '%s'.", face_name)

kafka_consumer: Optional[VisionKafkaConsumer] = None


def create_kafka_consumer() -> VisionKafkaConsumer:
    return VisionKafkaConsumer(
        scan_callback=scan_callback,
        save_face_callback=save_face_callback,
    )

# --- 3. FastAPI Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_consumer
    # STARTUP LOGIC
    logging.info("Vision Service Starting...")
    kafka_consumer = create_kafka_consumer()
    kafka_consumer.start()
    yield
    # SHUTDOWN LOGIC
    logging.info("Vision Service Shutting Down...")
    if kafka_consumer is not None:
        kafka_consumer.stop()
    with scan_lock:
        camera_controller.release()

# --- 4. FastAPI App Definition ---
app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    consumer_status = (
        kafka_consumer.status()
        if kafka_consumer is not None
        else {"running": False, "reason": "not_started"}
    )
    return {
        "status": "ok",
        "service": "vision",
        "kafka_consumer": consumer_status,
    }

# Optional: Add a manual trigger endpoint for testing without the Brain
@app.post("/scan/manual")
def trigger_manual_scan():
    scan_callback()
    return {"status": "Scan triggered"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=False)