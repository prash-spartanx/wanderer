import serial
import time
import logging
import threading

class SerialController:
    def __init__(self, port: str, baudrate: int = 115200):
        self.port = port
        self.baudrate = baudrate
        self.serial_conn = None
        self._lock = threading.RLock()
        self.ready = False
        self._ready_event = threading.Event()
        self.connect()

    def connect(self):
        """Attempts to open the serial port."""
        try:
            # We use a timeout of 0.1s so our reading functions don't hang forever 
            # if the Arduino suddenly unplugs or loses power.
            self.serial_conn = serial.Serial(self.port, self.baudrate, timeout=0.1)
            logging.info(f"Successfully connected to Arduino on {self.port}")
            threading.Thread(target=self._mark_ready_after_boot_delay, daemon=True).start()
        except serial.SerialException as e:
            logging.error(f"Failed to connect to Arduino: {e}")
            self.serial_conn = None

    def _mark_ready_after_boot_delay(self):
        """Lets the Arduino finish rebooting without blocking app startup."""
        time.sleep(2)
        self.ready = True
        self._ready_event.set()
        logging.info("Arduino boot delay elapsed; serial controller is ready.")

    def send_command(self, command: str) -> bool:
        """
        Takes a string command (e.g. 'FWD:140'), encodes it to bytes, 
        and writes it to the serial port.
        """
        with self._lock:
            if not self.ready:
                logging.warning("Serial controller is still booting; command deferred.")
                return False

            if not self.serial_conn or not self.serial_conn.is_open:
                logging.warning("Cannot send command, serial port is closed.")
                return False
                
            try:
                # 1. Add the newline terminator
                formatted_command = f"{command}\n"
                
                # 2. Encode string to bytes and write to the hardware buffer
                self.serial_conn.write(formatted_command.encode('utf-8'))
                
                # 3. Force the buffer to flush out immediately
                self.serial_conn.flush()
                return True
                
            except Exception as e:
                logging.error(f"Error sending command {command}: {e}")
                return False
        
    def read_line(self) -> str:
        """Reads a line of text from the serial port, if available."""
        with self._lock:
            if not self.ready:
                return ""

            if not self.serial_conn or not self.serial_conn.is_open:
                logging.warning("Cannot read line, serial port is closed.")
                return ""
            
            try:
                line = self.serial_conn.readline().decode('utf-8').strip()
                return line
            except Exception as e:
                logging.error(f"Error reading line from serial: {e}")
                return ""

    def send_and_read_line(self, command: str) -> str:
        """Sends one command and then reads a single response while holding the serial lock."""
        with self._lock:
            if not self.ready:
                logging.warning("Serial controller is still booting; exchange deferred.")
                return ""

            if not self.serial_conn or not self.serial_conn.is_open:
                logging.warning("Cannot communicate, serial port is closed.")
                return ""

            try:
                formatted_command = f"{command}\n"
                self.serial_conn.write(formatted_command.encode('utf-8'))
                self.serial_conn.flush()
                return self.serial_conn.readline().decode('utf-8').strip()
            except Exception as e:
                logging.error(f"Error during serial exchange for command {command}: {e}")
                return ""