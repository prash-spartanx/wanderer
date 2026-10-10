from openwakeword.model import Model
from openwakeword.utils import download_models
import sounddevice as sd
import numpy as np
import logging
import threading
import config

class WakeWordDetector:
    def __init__(
        self,
        model_name: str | None = None,
        threshold: float | None = None,
        input_device: int | None = None,
    ):
        logging.info("Initializing WakeWordDetector...")
        self.model_name = model_name or config.WAKE_WORD_MODEL
        self.threshold = threshold if threshold is not None else config.WAKE_WORD_THRESHOLD
        self.input_device = input_device if input_device is not None else config.AUDIO_INPUT_DEVICE
        download_models([self.model_name])
        self.model = Model(
            wakeword_models=[self.model_name],
            inference_framework="onnx",
        )
        self.stream = None
        self._stop_event = threading.Event()
        self._chunks_since_log = 0
        logging.info("WakeWordDetector initialized.")

    def _create_stream(self):
        self.stream = sd.InputStream(
            samplerate=16000,
            channels=1,
            dtype='int16',
            blocksize=1280,
            latency='low',
            device=self.input_device,
        )

    def _close_stream(self):
        if self.stream is None:
            return

        try:
            self.stream.stop()
        except Exception:
            pass

        try:
            self.stream.close()
        except Exception:
            pass

        self.stream = None

    def listen(self) -> bool:
        """Blocks and listens. Returns True exactly once when wake word is heard."""
        stream = None
        try:
            # A Model keeps rolling audio features and prediction history.
            # Without resetting it, the next listen() can replay the previous
            # detection before fresh microphone audio has been analyzed.
            self.model.reset()
            self._chunks_since_log = 0

            if self.stream is None:
                self._create_stream()

            # Keep a local reference: stop() may clear self.stream while this
            # thread is blocked in read().
            stream = self.stream
            if stream is None:
                logging.error("Could not create the wake-word audio stream.")
                return False

            self._stop_event.clear()
            stream.start()
            logging.info(
                "Listening for wake word on input device %s...",
                self.input_device if self.input_device is not None else "system default",
            )
            while not self._stop_event.is_set():
                audio, overflow = stream.read(1280) # openwakeword prefers chunk sizes of 1280

                if overflow:
                    logging.warning("Microphone input overflowed; wake-word audio may be incomplete.")

                # sounddevice returns (frames, channels); openwakeword expects flat int16 PCM.
                audio_flattened = np.asarray(audio, dtype=np.int16).reshape(-1)

                detected, score = self.detect(audio_flattened)
                self._chunks_since_log += 1
                if self._chunks_since_log >= 25:
                    rms = float(np.sqrt(np.mean(np.square(audio_flattened.astype(np.float32)))))
                    logging.info(
                        "Wake-word score: %.3f (threshold: %.3f, input RMS: %.1f, peak: %d)",
                        score,
                        self.threshold,
                        rms,
                        int(np.max(np.abs(audio_flattened))) if audio_flattened.size else 0,
                    )
                    if rms < 10:
                        logging.warning(
                            "Microphone input is nearly silent (RMS %.1f). "
                            "Check the selected input device, mute switch, and Windows microphone permissions.",
                            rms,
                        )
                    self._chunks_since_log = 0

                if detected:
                    logging.info("Wake word detected!")
                    self._close_stream() # Fully release the mic for the transcriber
                    return True
            return False
        except Exception as e:
            if self._stop_event.is_set():
                logging.debug("Wake-word listener stopped while reading audio: %s", e)
            else:
                logging.error(f"Error while listening: {e}")
            self._close_stream()
            return False
    
    def detect(self, audio: np.ndarray) -> tuple[bool, float]:
        try:
            scores = self.model.predict(audio)
            if not scores:
                return False, 0.0

            # A configured model can be returned under a normalized filename.
            matching_scores = [
                float(score)
                for key, score in scores.items()
                if self.model_name.lower() in key.lower()
            ]
            score = max(matching_scores or [float(max(scores.values()))])
            return score >= self.threshold, score
        except Exception as e:
            logging.error(f"Error while detecting wake word: {e}")
            return False, 0.0
    
    def stop(self):
        self._stop_event.set()
        try:
            self._close_stream()
            self.model.reset()
            logging.info("Hardware audio stream closed.")
        except Exception as e:
            logging.error(f"Error while stopping the stream: {e}")