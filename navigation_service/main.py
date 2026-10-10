import logging
import time
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Internal Imports
from kafka_producer import NavKafkaProducer
from kafka_consumer import NavKafkaConsumer
from wander_engine import WanderEngine
from serial_controller import SerialController
from sensor_reader import SensorReader
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global references for the hardware and engines
serial_ctrl = None
sensor_reader = None
wander_engine = None
kafka_producer = None
kafka_consumer = None
start_time = time.time()

# --- Pydantic Models for API Requests ---
class MotorTestRequest(BaseModel):
    command: str
    speed: int
    duration_ms: int

class ServoTestRequest(BaseModel):
    servo: str
    angle: int

# --- Application Lifecycle Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This runs exactly once when Docker/Uvicorn boots the app.
    It replaces the old 'main()' initialization block.
    """
    global serial_ctrl, sensor_reader, wander_engine, kafka_producer, kafka_consumer
    logger.info("=== Navigation Service Starting ===")

    try:
        # 1. Initialize Serial Connection
        serial_ctrl = SerialController(port=config.ARDUINO_PORT, baudrate=config.ARDUINO_BAUD)
        if not serial_ctrl.serial_conn:
            logger.error("Failed to connect to Arduino. Hardware endpoints will fail.")
        
        # 2. Initialize Sensor Reader
        sensor_reader = SensorReader(serial_ctrl, sense_interval_ms=config.SENSE_INTERVAL_MS)
        sensor_reader.start()
        
        # 3. Initialize Kafka Producer
        kafka_producer = NavKafkaProducer()
        
        # 4. Initialize Wander Engine
        wander_engine = WanderEngine(serial_ctrl, sensor_reader, kafka_producer)
        
        # 5. Initialize Kafka Consumer
        kafka_consumer = NavKafkaConsumer(wander_engine)
        kafka_consumer.start()
        
        logger.info("All components initialized successfully.")
        
        # Yield control back to FastAPI so it can start accepting HTTP requests
        yield 
        
    except Exception as e:
        logger.error(f"Fatal error during initialization: {e}", exc_info=True)
        raise e
    
    finally:
        # This block runs exactly once when Docker/Uvicorn shuts down the app (Ctrl+C)
        logger.info("Shutting down gracefully...")
        if wander_engine:
            wander_engine.stop()
        if sensor_reader:
            sensor_reader.stop()
        if kafka_consumer:
            kafka_consumer.stop()
        if kafka_producer:
            kafka_producer.close()
        if serial_ctrl and serial_ctrl.serial_conn:
            serial_ctrl.serial_conn.close()
        logger.info("All components shut down safely.")

# --- FastAPI App Initialization ---
app = FastAPI(lifespan=lifespan, title="Navigation Service")

# --- Endpoints ---

@app.get("/health")
def health_check():
    """Used by the Brain service to verify this container is alive."""
    uptime = int(time.time() - start_time)
    state = wander_engine.state if wander_engine else "UNKNOWN"
    return {"status": "ok", "wander_state": state, "uptime_seconds": uptime}

@app.get("/status")
def get_status():
    """Returns the full hardware state for debugging."""
    if not sensor_reader:
        raise HTTPException(status_code=503, detail="Hardware not initialized")
    
    return {
        "wander_state": wander_engine.state,
        "front_distance_cm": sensor_reader.get_distance(),
        "is_tilted": sensor_reader.get_is_tilted(),
        "arduino_connected": serial_ctrl.serial_conn is not None and serial_ctrl.serial_conn.is_open
    }

@app.post("/wander/start")
def start_wander():
    """Manual trigger to start the engine without Kafka."""
    if wander_engine:
        wander_engine.start()
        return {"message": "Wander engine started", "state": wander_engine.state}
    raise HTTPException(status_code=500, detail="Engine not ready")

@app.post("/wander/stop")
def stop_wander():
    """Manual trigger to stop the engine without Kafka."""
    if wander_engine:
        wander_engine.stop()
        wander_engine.state = "STOPPED"
        if serial_ctrl:
            serial_ctrl.send_command("STOP")
        return {"message": "Wander engine stopped"}
    raise HTTPException(status_code=500, detail="Engine not ready")

@app.post("/motor/test")
def test_motor(req: MotorTestRequest):
    """Direct motor control for hardware calibration."""
    if not serial_ctrl:
        raise HTTPException(status_code=503, detail="Serial controller offline")
    
    # Send command, e.g., "FWD:140"
    command_str = f"{req.command}:{req.speed}"
    success = serial_ctrl.send_command(command_str)
    
    if success:
        threading.Thread(
            target=_stop_motor_after_delay,
            args=(req.duration_ms,),
            daemon=True,
        ).start()
        return {"status": "accepted", "executed": command_str, "duration_ms": req.duration_ms}
    raise HTTPException(status_code=500, detail="Failed to send command to hardware")


def _stop_motor_after_delay(duration_ms: int):
    time.sleep(duration_ms / 1000.0)
    if serial_ctrl:
        serial_ctrl.send_command("STOP")

@app.post("/servo/test")
def test_servo(req: ServoTestRequest):
    """Direct servo control for positioning cameras or arms."""
    if not serial_ctrl:
        raise HTTPException(status_code=503, detail="Serial controller offline")
    
    # Map friendly names to Arduino commands
    servo_map = {
        "pan": "SERVO_PAN",
        "arm_left": "SERVO_ARM_L",
        "arm_right": "SERVO_ARM_R"
    }
    
    arduino_cmd = servo_map.get(req.servo.lower())
    if not arduino_cmd:
        raise HTTPException(status_code=400, detail=f"Unknown servo. Choose from {list(servo_map.keys())}")
    
    command_str = f"{arduino_cmd}:{req.angle}"
    success = serial_ctrl.send_command(command_str)
    
    if success:
        return {"status": "success", "executed": command_str}
    raise HTTPException(status_code=500, detail="Failed to send command to hardware")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=False,
    )