import time
import random
import logging
import threading
from serial_controller import SerialController
from sensor_reader import SensorReader
import config

STATE_EXPLORING = "EXPLORING"
STATE_REACTING = "REACTING"
STATE_DECIDING = "DECIDING"
STATE_PAUSED = "PAUSED"

class WanderEngine:
    def __init__(self, serial_ctrl: SerialController, sensor_reader: SensorReader, kafka_producer=None):
        self.serial = serial_ctrl
        self.sensor = sensor_reader
        self.kafka_producer = kafka_producer
        self.state = STATE_EXPLORING
        self.is_active = False
        self.thread = None
        self.lock = threading.Lock()
        self.manual_turn_thread = None
        self.manual_turn_lock = threading.Lock()
        self.vehicle_moving = False
        self.previously_stopped = False
        
        self.obstacle_threshold = config.OBSTACLE_THRESHOLD_CM
        self.fwd_speed = 140
        self.turn_speed = 120

    def start(self):
        should_turn_before_exploring = self.previously_stopped
        with self.lock:
            if self.thread and self.thread.is_alive():
                self.vehicle_moving = True
                self.state = STATE_EXPLORING
            else:
                self.is_active = True
                self.vehicle_moving = True
                self.state = STATE_EXPLORING
                self.thread = threading.Thread(target=self.run_loop, daemon=True)
                self.thread.start()
                logging.info("Wander Engine thread started.")

        if should_turn_before_exploring:
            direction = random.choice(["LEFT", "RIGHT"])
            logging.info(
                "Navigation restarted after stop; turning %s before exploring.",
                direction,
            )
            self.execute_manual_turn(direction)
        self.previously_stopped = True

    def stop(self):
        with self.lock:
            self.is_active = False
            self.vehicle_moving = False

        if self.thread and self.thread.is_alive():
            self.thread.join()

        self.thread = None
        logging.info("Wander Engine stopped.")

    def pause(self):
        """Called by the Kafka Consumer when Brain says PAUSE_WANDER"""
        self.vehicle_moving = False
        self.state = STATE_PAUSED
        self.serial.send_command("STOP")
        logging.info("Wander Engine PAUSED by Brain.")

    def resume(self):
        """Called by the Kafka Consumer when Brain says RESUME_WANDER"""
        self.vehicle_moving = True
        # THE FIX: Check the sensor before blindly going back to EXPLORING
        current_distance = self.sensor.get_distance()
        
        if current_distance < self.obstacle_threshold:
            logging.info("Resuming, but obstacle still present! Forcing a turn.")
            self.state = STATE_DECIDING # Skip EXPLORING, go straight to turning
        else:
            self.state = STATE_EXPLORING
            
        logging.info(f"Wander Engine RESUMED in state: {self.state}")

    def execute_manual_turn(self, direction: str):
        """Called when Brain explicitly sends ROTATE_LEFT or ROTATE_RIGHT"""
        with self.manual_turn_lock:
            if self.manual_turn_thread and self.manual_turn_thread.is_alive():
                logging.info("Manual turn already in progress.")
                return

            self.state = STATE_PAUSED # Pause normal loop while we force the turn
            self.manual_turn_thread = threading.Thread(
                target=self._run_manual_turn,
                args=(direction,),
                daemon=True,
            )
            self.manual_turn_thread.start()

    def _run_manual_turn(self, direction: str):
        if direction == "LEFT":
            self.serial.send_command(f"LEFT:{self.turn_speed}")
        else:
            self.serial.send_command(f"RIGHT:{self.turn_speed}")

        time.sleep(0.6) # Approximate 90-degree turn
        self.serial.send_command("STOP")
        self.state = STATE_EXPLORING # Safe to explore again

    def run_loop(self):
        """The core state machine loop."""
        while self.is_active:
            if not self.vehicle_moving or self.state == STATE_PAUSED:
                time.sleep(0.1)
                continue # Skip the rest of the loop until resumed
                
            distance = self.sensor.get_distance()
            is_tilted = self.sensor.get_is_tilted() 
            
            if is_tilted:
                logging.critical("Robot is tilted! Stopping motors.")
                self.serial.send_command("STOP")
                self.is_active = False
                break
                
            if self.state == STATE_EXPLORING:
                self._handle_exploring(distance)
                
            elif self.state == STATE_REACTING:
                self._handle_reacting()
                
            elif self.state == STATE_DECIDING:
                self._handle_deciding()
                
            time.sleep(0.05) 

    def _handle_exploring(self, current_distance: float):
        if not self.vehicle_moving:
            return

        if current_distance < self.obstacle_threshold:
            self.serial.send_command("STOP")
            self.vehicle_moving = False
            
            if self.kafka_producer:
                self.kafka_producer.publish_event(
                    "obstacle_detected",
                    distance_cm=current_distance,
                    confidence=None,
                )
            logging.info(
                "Obstacle detected at %.2f cm; published obstacle_detected to Brain.",
                current_distance,
            )
            self.state = STATE_PAUSED
        
    def _handle_reacting(self):
        """Back up slightly to clear the wheels."""
        self.serial.send_command(f"BWD:{self.fwd_speed}")
        time.sleep(0.4)
        self.serial.send_command("STOP")
        self.state = STATE_DECIDING
        logging.info("Obstacle reaction complete; deciding turn direction.")
        
    def _handle_deciding(self):
        """Pick a random direction, turn, and clear the path."""
        direction = random.choice(['LEFT', 'RIGHT'])
        if direction == 'LEFT':
            self.serial.send_command(f"LEFT:{self.turn_speed}")
        else:
            self.serial.send_command(f"RIGHT:{self.turn_speed}")
            
        time.sleep(0.6) # Turn duration
        self.serial.send_command("STOP")
        
        # TODO: Publish path_cleared to Kafka here
        self.state = STATE_EXPLORING
        logging.info("Turn complete; navigation engine is exploring.")