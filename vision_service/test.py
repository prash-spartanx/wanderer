"""Capture one camera frame and run the local vision pipeline.

Run from the project root with:
    python test.py
"""

import argparse
import importlib.util
import os
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture and inspect one vision frame")
    parser.add_argument(
        "--camera-index",
        type=int,
        default=int(os.getenv("CAMERA_INDEX", "0")),
        help="OpenCV camera index (default: CAMERA_INDEX or 0)",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("KNOWN_FACES_DB", "known_faces.pkl"),
        help="Known-face pickle path (default: KNOWN_FACES_DB or known_faces.pkl)",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OBJECT_MODEL", "yolov8n.pt"),
        help="YOLO model path (default: OBJECT_MODEL or yolov8n.pt)",
    )
    parser.add_argument(
        "--full-matrix",
        action="store_true",
        help="Print every pixel instead of NumPy's shortened matrix display",
    )
    parser.add_argument(
        "--face-attempts",
        type=int,
        default=int(os.getenv("FACE_ATTEMPTS", "5")),
        help="Number of camera frames to try for face detection (default: FACE_ATTEMPTS or 5)",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()

    print("[1/7] Loading vision libraries and pipeline components...")
    if importlib.util.find_spec("face_recognition_models") is None:
        print(
            "ERROR: face_recognition_models is incomplete in this Python environment. "
            "Its metadata is installed, but the importable package and model files are missing."
        )
        print(
            "The current environment cannot perform face detection or face matching until "
            "the model files are restored."
        )
        return 1

    try:
        import numpy as np

        from camera.camera_controller import CameraController
        from detection.embeddings_store import EmbeddingsStore
        from detection.face_detector import FaceDetector
        from detection.object_detector import ObjectDetector
    except SystemExit:
        print("ERROR: face_recognition stopped while loading its model files.")
        return 1
    except Exception as exc:
        print(f"ERROR: Could not load a required vision library: {exc}")
        print("Install the packages in requirements.txt and try again.")
        return 1

    camera = None
    try:
        print(f"[2/7] Opening camera with index {args.camera_index}...")
        camera = CameraController(args.camera_index)
        print("[3/7] Capturing one frame...")
        frame = camera.get_frame()
        if camera.cap is None or not camera.cap.isOpened():
            print(
                f"ERROR: Could not open camera {args.camera_index}. "
                "Check that it is connected, not in use, and permitted by Windows."
            )
            return 1
        print("      Camera opened successfully.")

        if frame is None:
            print("ERROR: Camera opened, but no frame could be read.")
            return 1

        if not isinstance(frame, np.ndarray) or frame.size == 0:
            print("ERROR: Camera returned an empty or invalid frame.")
            return 1

        print(
            f"      Frame captured: shape={frame.shape}, dtype={frame.dtype}, "
            f"min={frame.min()}, max={frame.max()}"
        )
        print("[4/7] Captured frame matrix (OpenCV BGR pixel values):")
        if args.full_matrix:
            with np.printoptions(threshold=sys.maxsize):
                print(frame)
        else:
            print(frame)
            print("      Matrix display is shortened by NumPy; use --full-matrix for all pixels.")

        print(f"[5/7] Loading known-face database: {args.database}")
        store = EmbeddingsStore(args.database)
        known_faces = store.load_known_faces()
        print(f"      Known faces available: {len(known_faces)}")

        print("[6/7] Passing frame to FaceDetector (BGR -> RGB -> face encoding -> DB match)...")
        face_detector = FaceDetector(store)
        face_results = []
        for attempt in range(1, max(1, args.face_attempts) + 1):
            if attempt > 1:
                next_frame = camera.get_frame()
                if isinstance(next_frame, np.ndarray) and next_frame.size > 0:
                    frame = next_frame

            face_results = face_detector.process_frame(frame)
            if face_results:
                if attempt > 1:
                    print(f"      Face found on camera attempt {attempt}.")
                break
            print(f"      No face found on camera attempt {attempt}/{max(1, args.face_attempts)}.")

        if face_results:
            print(f"      FaceDetector found {len(face_results)} face(s).")
            for number, result in enumerate(face_results, start=1):
                name = result.get("name", "unknown")
                confidence = result.get("confidence", 0.0)
                if name == "unknown":
                    print(f"      Face {number}: UNKNOWN (confidence={confidence})")
                    face_name = input("enter your name: ").strip()
                    if not face_name:
                        print("      Name was empty; face was not saved.")
                        continue

                    if store.save_new_face(face_name, result.get("embedding")):
                        print(f"      Saved face encoding for '{face_name}'.")
                    else:
                        print(f"      Failed to save face encoding for '{face_name}'.")
                else:
                    print(f"      Face {number}: MATCHED '{name}' (confidence={confidence})")
            return 0

        print(
            "      FaceDetector found no faces after the configured attempts. "
            "Move closer, improve lighting, and keep your face visible to the camera."
        )
        print(f"[7/7] Passing frame to ObjectDetector using model '{args.model}'...")
        object_label = ObjectDetector(args.model).process_frame(frame)
        if object_label:
            print(f"      Object detected: {object_label}")
        else:
            print("      No face or object detected.")
        return 0
    except Exception as exc:
        print(f"ERROR: Vision pipeline failed: {type(exc).__name__}: {exc}")
        return 1
    finally:
        if camera is not None:
            camera.release()
            print("Camera released.")


if __name__ == "__main__":
    raise SystemExit(run())