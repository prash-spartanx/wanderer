import logging
import threading
from queue import Queue
import pyttsx3

class Speaker:
    def __init__(self, engine_type: str = "pyttsx3"):
        self.engine_type = engine_type
        self._requests = Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._worker_loop,
            name="speech-tts-worker",
            daemon=True,
        )
        self._thread.start()

    def speak(self, text: str) -> bool:
        """Speak the given text using the selected TTS engine."""
        if not self._thread.is_alive() or self._stop_event.is_set():
            logging.error("TTS worker is not running.")
            return False

        result = []
        completed = threading.Event()
        self._requests.put((str(text), result, completed, None))
        if not completed.wait(timeout=120):
            logging.error("TTS worker timed out while speaking.")
            return False
        return bool(result and result[0])

    def speak_async(self, text: str, on_complete=None) -> bool:
        """Queue speech without blocking the caller."""
        if not self._thread.is_alive() or self._stop_event.is_set():
            logging.error("TTS worker is not running.")
            return False
        logging.info("Queueing speech request: %s", text)
        self._requests.put((str(text), [], None, on_complete))
        return True

    def stop(self) -> None:
        self._stop_event.set()
        self._requests.put(None)
        if self._thread.is_alive():
            self._thread.join(timeout=3)

    def _worker_loop(self):
        if self.engine_type != "pyttsx3":
            logging.error("Unsupported TTS engine type: %s", self.engine_type)
            self._ready_event.set()
            return

        while not self._stop_event.is_set():
            request = self._requests.get()
            if request is None:
                return

            text, result, completed, on_complete = request
            engine = None
            try:
                # Recreate SAPI for every request. Reusing a pyttsx3 engine can
                # leave its Windows event loop completed after the first utterance.
                engine = pyttsx3.init()
                logging.info(
                    "Speaking on thread %s: %s",
                    threading.current_thread().name,
                    text,
                )
                engine.say(text)
                engine.runAndWait()
                result.append(True)
            except Exception:
                logging.exception("Error during speech synthesis.")
                result.append(False)
            finally:
                if engine is not None:
                    try:
                        engine.stop()
                    except Exception:
                        logging.exception("Failed to stop the TTS engine.")
                if completed is not None:
                    completed.set()
                if on_complete is not None:
                    try:
                        on_complete(bool(result and result[0]))
                    except Exception:
                        logging.exception("TTS completion callback failed.")
        