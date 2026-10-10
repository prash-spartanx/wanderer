"""Manual microphone and Kafka test for the speech service.

Run this file from the speech_service directory and say "Hey Jarvis".
After the wake word is detected, speak a short command within the test
recording window. Set TEST_RECORD_SECONDS to change that window.
"""

import logging
import os
import sys
import traceback

import sounddevice as sd

import config
from kafka_producer import SpeechKafkaProducer
from stt.transcriber import AudioTranscriber
from stt.wake_word import WakeWordDetector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

RECORD_SECONDS = int(
    os.getenv("TEST_RECORD_SECONDS", "5")
)
TEST_INPUT_DEVICE = os.getenv("TEST_INPUT_DEVICE")


def run_test() -> int:
    logger.info("Starting speech service manual test")
    logger.info("Wake word: %s", config.WAKE_WORD_MODEL)
    logger.info("Wake-word threshold: %s", config.WAKE_WORD_THRESHOLD)
    logger.info("Command recording duration: %s seconds", RECORD_SECONDS)
    logger.info("Kafka bootstrap: %s", config.KAFKA_BOOTSTRAP)
    logger.info("Press Ctrl+C to stop the test.")

    wake_word = None
    try:
        logger.info("Loading wake-word detector...")
        logger.info("Available audio devices:")
        for index, device in enumerate(sd.query_devices()):
            if device["max_input_channels"] > 0:
                logger.info("  [%s] %s", index, device["name"])

        input_device = int(TEST_INPUT_DEVICE) if TEST_INPUT_DEVICE is not None else config.AUDIO_INPUT_DEVICE
        logger.info(
            "Using input device: %s",
            input_device if input_device is not None else "system default",
        )
        wake_word = WakeWordDetector(
            model_name=config.WAKE_WORD_MODEL,
            threshold=config.WAKE_WORD_THRESHOLD,
            input_device=input_device,
        )

        logger.info("Loading transcription model...")
        transcriber = AudioTranscriber(model_size=config.TRANSCRIPTION_MODEL_SIZE)
        producer = SpeechKafkaProducer()

        logger.info("Listening for wake word '%s'...", config.WAKE_WORD_MODEL)
        detected = wake_word.listen()
        logger.info("wake_word.listen() returned: %s", detected)

        if not detected:
            logger.error("Wake word was not detected.")
            return 1

        logger.info("Wake word detected. Publishing wake_word_detected event...")
        producer.publish_event(event="wake_word_detected")
        logger.info("wake_word_detected event published.")

        logger.info(
            "Recording now for %s seconds. Speak your command immediately.",
            RECORD_SECONDS,
        )
        audio_file = transcriber.record_audio(duration=RECORD_SECONDS)
        logger.info("record_audio() returned: %s", audio_file)

        if not audio_file:
            logger.error("Audio recording failed; transcription was skipped.")
            return 1

        logger.info("Transcribing recorded audio...")
        text = transcriber.transcribe(audio_file)
        logger.info("transcribe() returned: %r", text)

        if not text:
            logger.error("Transcription was empty; transcription_ready was skipped.")
            return 1

        logger.info("Publishing transcription_ready event...")
        producer.publish_event(event="transcription_ready", text=text)
        logger.info("transcription_ready event published with text: %r", text)
        logger.info("Speech pipeline test completed successfully.")
        return 0

    except KeyboardInterrupt:
        logger.info("Test stopped by user.")
        return 130
    except Exception:
        logger.exception("Speech pipeline test failed:")
        traceback.print_exc()
        return 1
    finally:
        if wake_word is not None:
            logger.info("Closing wake-word audio stream...")
            wake_word.stop()


if __name__ == "__main__":
    sys.exit(run_test())