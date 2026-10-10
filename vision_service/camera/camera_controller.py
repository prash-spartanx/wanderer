import cv2
import logging
import numpy as np
import threading
from typing import Optional

class CameraController:
    def __init__(self, camera_index: int = 0):
        self.camera_index = camera_index
        self.cap = None
        self._lock = threading.Lock()

    def _open(self) -> bool:
        if self.cap is not None and self.cap.isOpened():
            return True

        self.cap = cv2.VideoCapture(self.camera_index)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)

        if not self.cap.isOpened():
            logging.error(
                f"CRITICAL: Failed to open camera with index {self.camera_index}. "
                "Check hardware connection."
            )
            return False

        logging.info(f"Camera {self.camera_index} initialized successfully.")
        return True
    
    def get_frame(self) -> Optional[np.ndarray]:
        with self._lock:
            if not self._open():
                return None

            ret, frame = self.cap.read()
            if not ret:
                logging.warning("Dropped frame: Failed to read from camera buffer.")
                return None

            return frame
        
    def release(self) -> None:
        with self._lock:
            if self.cap is not None and self.cap.isOpened():
                self.cap.release()
                logging.info(f"Camera {self.camera_index} released safely.")
            self.cap = None