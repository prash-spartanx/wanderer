from ultralytics import YOLO


class ObjectDetector:
    def __init__(self, model_path: str = "yolov8n.pt", threshold: float = 0.5):
        self.model = YOLO(model_path)
        self.threshold = threshold

    def detect_objects(self, frame):
        """Return all detected objects above the confidence threshold."""
        results = self.model(frame)
        result = results[0]

        labels = []
        for box in result.boxes:
            confidence = float(box.conf[0])
            if confidence < self.threshold:
                continue

            class_id = int(box.cls[0])
            labels.append({
                "objectName": result.names[class_id],
                "confidence": round(confidence, 2),
            })

        return labels

    def process_frame(self, frame):
        """Compatibility wrapper used by main.py."""
        labels = self.detect_objects(frame)
        return labels[0] if labels else None
