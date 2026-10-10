"""Central configuration for the speech service."""

from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "speech-service")
KAFKA_TOPIC_SPEECH_EVENTS = os.getenv("KAFKA_TOPIC_SPEECH_EVENTS", "speech-events")
KAFKA_TOPIC_BRAIN_COMMANDS = os.getenv("KAFKA_TOPIC_BRAIN_COMMANDS", "brain-commands")
KAFKA_TOPIC_TASK_COMPLETIONS = os.getenv("KAFKA_TOPIC_TASK_COMPLETIONS", "task-completions")

WAKE_WORD_MODEL = os.getenv("WAKE_WORD_MODEL", "hey_jarvis")
WAKE_WORD_THRESHOLD = float(os.getenv("WAKE_WORD_THRESHOLD", "0.5"))
AUDIO_INPUT_DEVICE = os.getenv("AUDIO_INPUT_DEVICE", "").strip()
AUDIO_INPUT_DEVICE = int(AUDIO_INPUT_DEVICE) if AUDIO_INPUT_DEVICE else None

TRANSCRIPTION_MODEL_SIZE = os.getenv("TRANSCRIPTION_MODEL_SIZE", "base")
IDENTIFICATION_DURATION_SECONDS = int(os.getenv("IDENTIFICATION_DURATION_SECONDS", "15"))
COMMAND_DURATION_SECONDS = int(os.getenv("COMMAND_DURATION_SECONDS", "30"))

SERVICE_HOST = os.getenv("SPEECH_SERVICE_HOST", "0.0.0.0")
SERVICE_PORT = int(os.getenv("SPEECH_SERVICE_PORT", "8003"))
