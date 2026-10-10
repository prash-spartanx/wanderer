"""Hardware test for the Arduino ultrasonic distance sensor.

Expected Arduino wiring:
    Sensor VCC  -> Arduino 5V
    Sensor GND  -> Arduino GND
    Sensor TRIG -> Arduino D9
    Sensor ECHO -> Arduino D8

The Arduino sketch must answer each ``PING`` command with a line such as
``DIST:27.4`` where the value is the measured distance in centimeters.
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from typing import Optional

import serial
from serial.tools import list_ports


DEFAULT_PORT = "COM5"
DEFAULT_BAUDRATE = 115200
DEFAULT_TIMEOUT_SECONDS = 1.0
DEFAULT_INTERVAL_SECONDS = 0.5
DEFAULT_RETRIES = 3


def parse_distance(line: str) -> Optional[float]:
    """Return a distance from an Arduino response, or None for other lines."""
    prefix, separator, value = line.partition(":")
    if separator != ":" or prefix.strip() != "DIST":
        return None

    try:
        distance_cm = float(value.strip())
    except ValueError:
        logging.warning("Invalid distance value received: %r", line)
        return None

    if not math.isfinite(distance_cm) or distance_cm < 0:
        logging.warning("Non-finite or negative distance received: %r", line)
        return None

    return distance_cm


def show_available_ports() -> None:
    """Print detected serial ports to help diagnose a wrong COM port."""
    ports = list(list_ports.comports())
    if not ports:
        print("No serial ports were detected.")
        return

    print("Detected serial ports:")
    for port in ports:
        description = port.description or "unknown device"
        print(f"  {port.device}: {description}")


def read_distance(
    connection: serial.Serial,
    timeout_seconds: float,
) -> tuple[Optional[float], bool]:
    """Request one measurement and return it plus whether the sensor timed out."""
    connection.reset_input_buffer()
    connection.write(b"PING\n")
    connection.flush()

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        raw_line = connection.readline()
        if not raw_line:
            continue

        line = raw_line.decode("utf-8", errors="replace").strip()
        if line == "DIST:-1":
            return None, True

        distance_cm = parse_distance(line)
        if distance_cm is not None:
            return distance_cm, False

        print(f"Arduino response: {line!r}")

    return None, False


def run_test(
    port: str,
    baudrate: int,
    count: int,
    interval_seconds: float,
    timeout_seconds: float,
    retries: int,
) -> int:
    """Connect to the Arduino and print the requested number of readings."""
    print(f"Opening {port} at {baudrate} baud...")
    try:
        with serial.Serial(
            port=port,
            baudrate=baudrate,
            timeout=0.1,
            write_timeout=timeout_seconds,
        ) as connection:
            # Opening a Uno serial connection commonly resets the board.
            time.sleep(2.0)
            connection.reset_input_buffer()
            print("Connected. Requesting distance readings...")

            successful_readings = 0
            for reading_number in range(1, count + 1):
                distance_cm = None
                saw_sensor_timeout = False
                for attempt in range(1, retries + 1):
                    distance_cm, sensor_timeout = read_distance(
                        connection, timeout_seconds
                    )
                    saw_sensor_timeout = saw_sensor_timeout or sensor_timeout
                    if distance_cm is not None:
                        break
                    if attempt < retries:
                        time.sleep(0.05)

                if distance_cm is None:
                    if saw_sensor_timeout:
                        message = (
                            f"no echo after {retries} attempts; "
                            "check sensor alignment or target distance"
                        )
                    else:
                        message = (
                            f"no valid DIST response within "
                            f"{timeout_seconds:.1f}s"
                        )
                    print(f"[{reading_number}/{count}] WARNING: {message}")
                else:
                    successful_readings += 1
                    print(
                        f"[{reading_number}/{count}] "
                        f"distance = {distance_cm:.2f} cm"
                    )

                if reading_number < count:
                    time.sleep(interval_seconds)

    except serial.SerialException as error:
        print(f"ERROR: could not open or use {port}: {error}", file=sys.stderr)
        show_available_ports()
        return 1

    if successful_readings == count:
        print("PASS: all requested distance readings were received.")
        return 0

    print(
        f"FAIL: received {successful_readings}/{count} valid distance readings.",
        file=sys.stderr,
    )
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test Arduino ultrasonic distance readings over serial."
    )
    parser.add_argument("--port", default=DEFAULT_PORT, help="Serial port (default: COM5)")
    parser.add_argument(
        "--baudrate",
        type=int,
        default=DEFAULT_BAUDRATE,
        help="Serial baud rate (default: 115200)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of readings to request (default: 10)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Seconds between readings (default: 0.5)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Seconds to wait for each reading (default: 1.0)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Attempts per reading after a missed echo (default: 3)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be at least 1")
    if args.interval < 0 or args.timeout <= 0 or args.retries < 1:
        parser.error(
            "--interval must be non-negative, --timeout must be positive, "
            "and --retries must be at least 1"
        )

    return run_test(
        port=args.port,
        baudrate=args.baudrate,
        count=args.count,
        interval_seconds=args.interval,
        timeout_seconds=args.timeout,
        retries=args.retries,
    )


if __name__ == "__main__":
    raise SystemExit(main())
