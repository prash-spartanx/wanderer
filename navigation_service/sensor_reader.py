import threading
import time
import logging
from serial_controller import SerialController

class SensorState:
    def __init__(self):
        self.front_distance_cm = 400.0  # Default to max distance
        self.is_tilted = False
        self.last_updated = 0.0
        self.lock = threading.Lock()    # The critical mutex lock

class SensorReader:
    def __init__(self, serial_ctrl: SerialController, sense_interval_ms: int = 100):
        self.serial = serial_ctrl
        self.interval = sense_interval_ms / 1000.0
        self.state = SensorState()
        self.running = False
        self.thread = None

    def start(self):
        """Spins up the background thread."""
        if self.thread is None or not self.thread.is_alive():
            self.running = True
            self.thread = threading.Thread(target=self._read_loop, daemon=True)
            self.thread.start()
            logging.info("Sensor reader thread started.")

    def stop(self):
        """Signals the thread to shut down."""
        self.running = False
        if self.thread:
            self.thread.join()
            logging.info("Sensor reader thread stopped.")

    def get_distance(self) -> float:
        """Thread-safe read for the Wander Engine to use."""
        with self.state.lock:
            return self.state.front_distance_cm
    def get_is_tilted(self) -> bool:
        """Thread-safe read for the Wander Engine to use."""
        with self.state.lock:
            return self.state.is_tilted
    def _read_loop(self):
        """The continuous background loop."""
        while self.running:
            if (
                self.serial.ready
                and self.serial.serial_conn
                and self.serial.serial_conn.is_open
            ):
                # 1. Ask Arduino for data
                line = self.serial.send_and_read_line("PING")
                
                if line:
                    self._parse_and_update(line)
            
            time.sleep(self.interval)

    def _parse_and_update(self, line: str):
        """Parses the line and safely updates the shared sensor state."""
        # 1. Split the string to separate the prefix from the data (e.g., "DIST" and "25.5")
        parts = line.split(":")
        
        # If there is no colon, it's not a valid data string, so we ignore it
        if len(parts) != 2:
            return 

        prefix = parts[0]
        data = parts[1]

        try:
            # 2. CRITICAL: Acquire the lock BEFORE modifying the shared state
            with self.state.lock:
                
                # 3. Handle Ultrasonic Distance Data
                if prefix == "DIST":
                    distance_cm = float(data)
                    if distance_cm < 0:
                        return

                    self.state.front_distance_cm = distance_cm
                    self.state.last_updated = time.time()
                    
                # 4. Handle MPU-6050 Gyro/Accel Data
                elif prefix == "GYRO":
                    # The data looks like "0.1,0.02,0.98,4.5,-2.1,0.0"
                    # We split it by the comma to get a list of the 6 numbers
                    gyro_values = data.split(",")
                    
                    if len(gyro_values) == 6:
                        # Z-axis acceleration is the 3rd value in the list (index 2)
                        accel_z = float(gyro_values[2])
                        
                        # If Z-axis gravity drops below 0.5g or spikes above 1.5g, it is tilted
                        if accel_z < 0.5 or accel_z > 1.5:
                            self.state.is_tilted = True
                        else:
                            self.state.is_tilted = False
                            
                        self.state.last_updated = time.time()

        except ValueError:
            # This safely catches errors if the Arduino sends corrupted numbers like "DIST:abc"
            logging.warning(f"Received non-numeric sensor data in line: '{line}'")
