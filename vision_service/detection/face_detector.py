import cv2
from detection.embeddings_store import EmbeddingsStore
import face_recognition
import numpy as np
import logging

class FaceDetector:
    def __init__(self, store: EmbeddingsStore):
        self.store = store
    
    def process_frame(self, frame: np.ndarray) -> list[dict]:
        # Action 1: Convert BGR (OpenCV) to RGB (face_recognition)
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        
        # Action 2: Find all faces in the current frame
        face_locations = face_recognition.face_locations(rgb_frame)
        
        # Action 3: Compute encodings for every face found
        face_encodings = face_recognition.face_encodings(rgb_frame, face_locations)

        known_dict = self.store.load_known_faces()
        known_names = list(known_dict.keys())
        known_encodings = list(known_dict.values())
        
        results = []

        # Action 4: Match EACH detected face against our database
        for face_encoding in face_encodings:
            best_match = "unknown"
            confidence = 0.0
            
            if known_encodings:
                # face_distance calculates the Euclidean distance against an array of knowns instantly
                face_distances = face_recognition.face_distance(known_encodings, face_encoding)
                
                # Find the index of the lowest distance
                best_match_index = np.argmin(face_distances)
                min_distance = face_distances[best_match_index]
                
                # The library author recommends a strict <= 0.6 threshold for a match
                if min_distance <= 0.6:
                    best_match = known_names[best_match_index]
                    # Convert distance to a pseudo-confidence score (1.0 is perfect, 0.4 is the cutoff)
                    confidence = round(1.0 - min_distance, 2)
            
            # Append the result for THIS specific face to our list
            results.append({
                "name": best_match,
                "confidence": confidence,
                "embedding": face_encoding,
            })

        return results