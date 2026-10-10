"""Standalone microphone-to-Whisper test.

This test does not load openWakeWord, Kafka, FastAPI, or the service pipeline.
It only records from the microphone and transcribes the recorded audio.

Examples:
    python test_transcription.py
    $env:TEST_RECORD_SECONDS = "10"; python test_transcription.py
    $env:TEST_INPUT_DEVICE = "2"; python test_transcription.py

For cmd.exe, use:
    set TEST_INPUT_DEVICE=2
    python test_transcription.py
"""

import logging
import os
import sys
import traceback

import sounddevice as sd

import config
from stt.transcriber import AudioTranscriber


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

RECORD_SECONDS = int(os.getenv("TEST_RECORD_SECONDS", "10"))
INPUT_DEVICE = os.getenv("TEST_INPUT_DEVICE")


def run_test() -> int:
    logger.info("Starting microphone transcription test")
    logger.info("Recording duration: %s seconds", RECORD_SECONDS)
    logger.info("Whisper model: %s", config.TRANSCRIPTION_MODEL_SIZE)

    try:
        devices = sd.query_devices()
        logger.info("Available audio devices:")
        for index, device in enumerate(devices):
            if device["max_input_channels"] > 0:
                logger.info(
                    "  [%s] %s | inputs=%s | default_samplerate=%s",
                    index,
                    device["name"],
                    device["max_input_channels"],
                    device["default_samplerate"],
                )

        if INPUT_DEVICE is not None:
            sd.default.device = int(INPUT_DEVICE)
            logger.info("Using input device index: %s", INPUT_DEVICE)
        else:
            logger.info("Using the system default input device.")

        logger.info("Loading Whisper model...")
        transcriber = AudioTranscriber(model_size=config.TRANSCRIPTION_MODEL_SIZE)

        logger.info("Recording starts now. Speak clearly...")
        audio_file = transcriber.record_audio(duration=RECORD_SECONDS)
        logger.info("record_audio() returned: %r", audio_file)

        if not audio_file:
            logger.error("Recording failed. No audio file was created.")
            return 1

        logger.info("Transcribing audio...")
        text = transcriber.transcribe(audio_file)
        logger.info("Transcription result: %r", text)
        print("\nTRANSCRIPTION:")
        print(text or "<empty>")
        return 0 if text else 1

    except KeyboardInterrupt:
        logger.info("Test stopped by user.")
        return 130
    except Exception:
        logger.exception("Microphone transcription test failed:")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(run_test())
