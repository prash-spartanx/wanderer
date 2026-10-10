FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# PortAudio is required by sounddevice, eSpeak provides the Linux TTS backend
# used by pyttsx3, and ffmpeg is required by Whisper.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        libespeak1 \
        libportaudio2 \
        portaudio19-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install --index-url https://download.pytorch.org/whl/cpu torch==2.14.0 \
    && python -m pip install -r requirements.txt

COPY . .

EXPOSE 8003

CMD ["python", "main.py"]
