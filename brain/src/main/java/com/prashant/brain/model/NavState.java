package com.prashant.brain.model;

public enum NavState {
    EXPLORING,      // Actively moving / wandering
    PAUSED_SPEECH,  // Paused because robot is listening or speaking
    BLOCKED,        // Stopped due to an obstacle
    IDLE            // Stopped by user command
}