import os

from dotenv import load_dotenv

load_dotenv()

# Get the environment variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_VISION_EVENTS = os.getenv("KAFKA_TOPIC", "vision-events")
KAFKA_TOPIC_BRAIN_COMMANDS = os.getenv("KAFKA_TOPIC_BRAIN_COMMANDS", "brain-commands")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "vision-service")
CAMERA_INDEX = int(os.getenv("CAMERA_INDEX", 0))