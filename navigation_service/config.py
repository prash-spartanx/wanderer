import os
from dotenv import load_dotenv

# Load variables from .env file into the environment
load_dotenv()

# Kafka Settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_NAV_EVENTS = os.getenv("KAFKA_TOPIC_NAV_EVENTS", "nav-events")
KAFKA_TOPIC_BRAIN_COMMANDS = os.getenv("KAFKA_TOPIC_BRAIN_COMMANDS", "brain-commands")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "navigation-service")

# Arduino and sensor settings
ARDUINO_PORT = os.getenv("ARDUINO_PORT", "COM5")
ARDUINO_BAUD = int(os.getenv("ARDUINO_BAUD", "115200"))
SENSE_INTERVAL_MS = int(os.getenv("SENSE_INTERVAL_MS", "100"))
OBSTACLE_THRESHOLD_CM = float(os.getenv("OBSTACLE_THRESHOLD_CM", "25.0"))
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))