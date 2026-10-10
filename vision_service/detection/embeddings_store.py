import pickle
import os
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)

class EmbeddingsStore:
    def __init__(self, db_path: str = "known_faces.pkl"):
        self.db_path = db_path
        self.embeddings = {}
        # FIX: Call the correct method name
        self.load_known_faces() 
    
    def load_known_faces(self) -> dict:
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, 'rb') as f:
                    self.embeddings = pickle.load(f)
                    logging.info(f"Loaded {len(self.embeddings)} known faces from {self.db_path}.")
            except Exception as e:
                logging.error(f"Failed to load known faces from {self.db_path}: {e}")
                self.embeddings = {}
        else:
            logging.info("No existing embeddings database found. Initialized empty store.")
            self.embeddings = {}
            
        return self.embeddings
    
    def save_new_face(self, name: str, embedding: np.ndarray) -> bool:
        if embedding is None or not isinstance(embedding, np.ndarray):
            logging.error("Invalid embedding provided. Must be a non-empty numpy array.")
            return False
            
        cur_embeddings = self.load_known_faces()
        
        if name in cur_embeddings:
            logging.info(f"Face with name '{name}' already exists. Overwriting the embedding.")
        else:
            logging.info(f"Adding new face '{name}' to the embeddings store.")
            
        cur_embeddings[name] = embedding
        
        # FIX: Actually write the updated dictionary back to the disk!
        try:
            with open(self.db_path, 'wb') as f:
                pickle.dump(cur_embeddings, f)
            return True
        except Exception as e:
            logging.error(f"Failed to save embeddings to {self.db_path}: {e}")
            return False