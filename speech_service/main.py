import logging
import re
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn

# Local imports
import config
from tts.speaker import Speaker
from stt.wake_word import WakeWordDetector
from stt.transcriber import AudioTranscriber
from kafka_producer import SpeechKafkaProducer
from kafka_consumer import SpeechKafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. Global Instances ---
speaker = Speaker(engine_type="pyttsx3")
wake_word = WakeWordDetector(model_name=config.WAKE_WORD_MODEL, threshold=config.WAKE_WORD_THRESHOLD)
transcriber = AudioTranscriber(model_size=config.TRANSCRIPTION_MODEL_SIZE)
kafka_producer = SpeechKafkaProducer()

# Thread control flag
audio_loop_active = False
audio_thread = None
service_active = False


def start_audio_pipeline():
    global audio_loop_active, audio_thread, service_active


    if not service_active:
        return

    if audio_loop_active and audio_thread is not None and audio_thread.is_alive():
        return

    audio_loop_active = True
    audio_thread = threading.Thread(target=audio_pipeline_loop, daemon=True)
    audio_thread.start()


def stop_audio_pipeline(wait_for_thread: bool = True):
    global audio_loop_active, audio_thread

    audio_loop_active = False
    wake_word.stop()

    if wait_for_thread and audio_thread is not None and audio_thread.is_alive():
        audio_thread.join(timeout=2)

    audio_thread = None

# --- 2. The Mouth: Consumer Callback ---
def speak_callback(text: str):
    """Triggered by the Kafka Consumer when Brain sends speek."""
    logging.info(f"Commanded to speak: '{text}'")

    queued = speaker.speak_async(
        text,
        on_complete=lambda success: kafka_producer.publish_task_completion(
            task_name="speak",
            status="success" if success else "failure",
        ),
    )
    if not queued:
        kafka_producer.publish_task_completion(task_name="speak", status="failure")


def extract_name_from_transcript(transcript: str) -> str:
    cleaned = transcript.strip()
    if not cleaned:
        return ""

    patterns = [
        r"(?i)\bmy name is\s+(.+)$",
        r"(?i)\bi am\s+(.+)$",
        r"(?i)\bi[' ]?m\s+(.+)$",
        r"(?i)\bit[' ]?s\s+(.+)$",
        r"(?i)\bthis is\s+(.+)$",
    ]

    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            cleaned = match.group(1)
            break

    cleaned = re.split(r"[.?!,;:]", cleaned, maxsplit=1)[0]
    cleaned = re.sub(r"(?i)\b(please|thank you|thanks|sir|ma'am|madam)\b", "", cleaned)
    return " ".join(cleaned.split()).strip("\"' ")


def identify_callback(text: str):
    """Triggered by the Kafka Consumer when Brain sends IDENTIFY."""
    logging.info(f"Commanded to identify using prompt: '{text}'")

    stop_audio_pipeline(wait_for_thread=True)

    try:
        if not speaker.speak(text):
            logging.warning("IDENTIFY prompt could not be spoken.")
            return

        audio_file = transcriber.record_audio(duration=config.IDENTIFICATION_DURATION_SECONDS)
        if not audio_file:
            logging.warning("IDENTIFY recording failed.")
            return

        transcript = transcriber.transcribe(audio_file)
        if not transcript:
            logging.warning("IDENTIFY transcription failed or returned empty.")
            return

        name = extract_name_from_transcript(transcript)
        if not name:
            logging.warning("IDENTIFY transcription did not contain a usable name: %s", transcript)
            return

        logging.info("Identified person name: %s", name)
        kafka_producer.publish_event(event="person_identified", text=name)
    finally:
        start_audio_pipeline()


# Instantiate consumer with the callback
kafka_consumer = SpeechKafkaConsumer(speak_callback=speak_callback, identify_callback=identify_callback)


# --- 3. The Ears: Background Audio Pipeline ---
def audio_pipeline_loop():
    """Sequential loop handling hardware handoff between Wake Word and STT."""
    global audio_loop_active
    
    while audio_loop_active:
        logging.info("Audio pipeline ready. Waiting for wake word...")
        
        # 1. Block and listen (mic opens)
        detected = wake_word.listen()
        
        if detected and audio_loop_active:
            # 2. Tell the Brain to stop the robot
            kafka_producer.publish_event(event="wake_word_detected")
            
            # 3. Record command (mic opens again, then closes)
            audio_file = transcriber.record_audio(duration=config.COMMAND_DURATION_SECONDS)
            
            if audio_file:
                # 4. Transcribe (Heavy ML compute)
                text = transcriber.transcribe(audio_file)
                
                if text:
                    logging.info(f"User said: {text}")
                    # 5. Send command to Brain
                    kafka_producer.publish_event(event="transcription_ready", text=text)
                else:
                    logging.warning("Transcription failed or returned empty.")


# --- 4. FastAPI Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global service_active
    logging.info("Speech Service Starting...")
    
    # Start Kafka Consumer
    kafka_consumer.start()
    
    # Start Audio Pipeline Thread
    service_active = True
    start_audio_pipeline()
    
    yield # Let FastAPI run
    
    # Shutdown Logic
    logging.info("Speech Service Shutting Down...")
    service_active = False
    stop_audio_pipeline(wait_for_thread=True)
    kafka_consumer.stop()
    speaker.stop()

# --- 5. FastAPI App Definition ---
app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "speech",
        "kafka_consumer_running": kafka_consumer.is_running,
        "kafka_consumer_thread": (
            kafka_consumer.thread.name if kafka_consumer.thread else None
        ),
        "kafka_command_handler_thread": (
            kafka_consumer.handler_thread_name
        ),
        "kafka_command_handler_running": kafka_consumer.is_handler_running,
    }

# Manual trigger to test the mouth without the Brain
@app.post("/speak/test")
def trigger_manual_speak(text: str = "This is a test of the speech system."):
    speak_callback(text)
    return {"status": "Speech executed"}

if __name__ == "__main__":
    uvicorn.run(app, host=config.SERVICE_HOST, port=config.SERVICE_PORT, reload=False)