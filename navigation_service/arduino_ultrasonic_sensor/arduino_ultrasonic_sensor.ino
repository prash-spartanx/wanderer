/*
  Arduino Uno ultrasonic sensor firmware for navigation_service.

  Wiring:
    HC-SR04 VCC  -> Arduino 5V
    HC-SR04 GND  -> Arduino GND
    HC-SR04 TRIG -> Arduino D9
    HC-SR04 ECHO -> Arduino D8

  Serial protocol at 115200 baud:
    Python sends: PING
    Arduino sends: DIST:<centimeters>
*/

#include <string.h>

const uint8_t TRIG_PIN = 9;
const uint8_t ECHO_PIN = 8;
const unsigned long SERIAL_BAUD = 115200;
const unsigned long ECHO_TIMEOUT_US = 30000;
const uint8_t COMMAND_BUFFER_SIZE = 16;
const uint8_t MEASUREMENT_ATTEMPTS = 3;

void setup() {
  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  digitalWrite(TRIG_PIN, LOW);
  Serial.begin(SERIAL_BAUD);
}

float measureDistanceCm() {
  for (uint8_t attempt = 0; attempt < MEASUREMENT_ATTEMPTS; attempt++) {
    digitalWrite(TRIG_PIN, LOW);
    delayMicroseconds(2);
    digitalWrite(TRIG_PIN, HIGH);
    delayMicroseconds(10);
    digitalWrite(TRIG_PIN, LOW);

    unsigned long echoDuration = pulseIn(ECHO_PIN, HIGH, ECHO_TIMEOUT_US);
    if (echoDuration > 0) {
      return (echoDuration * 0.0343) / 2.0;
    }

    delay(5);
  }

  return -1.0;
}

void loop() {
  static char command[COMMAND_BUFFER_SIZE];
  static uint8_t commandLength = 0;

  while (Serial.available() > 0) {
    char incoming = Serial.read();

    if (incoming == '\r') {
      continue;
    }

    if (incoming != '\n') {
      if (commandLength < COMMAND_BUFFER_SIZE - 1) {
        command[commandLength++] = incoming;
      }
      continue;
    }

    command[commandLength] = '\0';
    commandLength = 0;

    if (strcmp(command, "PING") != 0) {
      Serial.println("ERR:UNKNOWN_COMMAND");
      continue;
    }

    float distanceCm = measureDistanceCm();
    if (distanceCm < 0) {
      Serial.println("DIST:-1");
    } else {
      Serial.print("DIST:");
      Serial.println(distanceCm, 2);
    }
  }
}
