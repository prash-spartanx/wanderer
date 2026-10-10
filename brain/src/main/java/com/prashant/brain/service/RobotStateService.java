package com.prashant.brain.service;

import com.prashant.brain.model.IntentMode;
import com.prashant.brain.model.NavState;
import com.prashant.brain.model.SpeechState;
import com.prashant.brain.model.VisionState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RobotStateService {
    private final StringRedisTemplate redisTemplate;

    private static final String INTENDED_MODE_KEY = "robot:state:intended_mode";
    private static final String NAV_STATE_KEY = "robot:state:navigation";
    private static final String SPEECH_STATE_KEY = "robot:state:speech";
    private static final String VISION_STATE_KEY = "robot:state:vision";

    public RobotStateService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // --- Navigation ---
    public void updateNavigationState(NavState state) {
        if (state == null) return;
        redisTemplate.opsForValue().set(NAV_STATE_KEY, state.name());
    }

    public NavState getNavigationState() {
        String state = redisTemplate.opsForValue().get(NAV_STATE_KEY);
        return parseEnumSafely(NavState.class, state, NavState.IDLE);
    }

    public boolean isExploring() {
        return getNavigationState() == NavState.EXPLORING;
    }

    // --- Speech ---
    public void updateSpeechState(SpeechState state) {
        if (state == null) return;
        redisTemplate.opsForValue().set(SPEECH_STATE_KEY, state.name());
    }

    public SpeechState getSpeechState() {
        String state = redisTemplate.opsForValue().get(SPEECH_STATE_KEY);
        return parseEnumSafely(SpeechState.class, state, SpeechState.IDLE);
    }

    public boolean isSpeechBusy() {
        SpeechState state = getSpeechState();
        return state == SpeechState.SPEAKING
                || state == SpeechState.LISTENING
                || state == SpeechState.THINKING;
    }

    // --- Vision ---
    public void updateVisionState(VisionState state) {
        if (state == null) return;
        redisTemplate.opsForValue().set(VISION_STATE_KEY, state.name());
    }

    public VisionState getVisionState() {
        String state = redisTemplate.opsForValue().get(VISION_STATE_KEY);
        return parseEnumSafely(VisionState.class, state, VisionState.IDLE);
    }

    public boolean isVisionBusy() {
        VisionState state = getVisionState();
        return state == VisionState.SCANNING || state == VisionState.SAVING;
    }

    // --- Intended Mode ---
    public void setIntendedMode(IntentMode mode) {
        if (mode == null) return;
        redisTemplate.opsForValue().set(INTENDED_MODE_KEY, mode.name());
    }

    public IntentMode getIntendedMode() {
        String mode = redisTemplate.opsForValue().get(INTENDED_MODE_KEY);
        return parseEnumSafely(IntentMode.class, mode, IntentMode.IDLE);
    }

    // --- Master Lock ---
    public boolean isSafeToMove() {
        return !isSpeechBusy() && !isVisionBusy();
    }

    // --- Boot / Recovery Cleanup ---
    public void resetAllStates() {
        log.info("Resetting all robot states in Redis to IDLE");
        updateNavigationState(NavState.IDLE);
        updateSpeechState(SpeechState.IDLE);
        updateVisionState(VisionState.IDLE);
        setIntendedMode(IntentMode.IDLE);
    }

    private <T extends Enum<T>> T parseEnumSafely(Class<T> enumClass, String value, T defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            log.error("Invalid state value '{}' for enum {}. Defaulting to {}", value, enumClass.getSimpleName(), defaultValue);
            return defaultValue;
        }
    }
}