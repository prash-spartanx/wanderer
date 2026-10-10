package com.prashant.brain.controller;

import com.prashant.brain.model.IntentMode;
import com.prashant.brain.model.NavState;
import com.prashant.brain.model.SpeechState;
import com.prashant.brain.model.VisionState;
import com.prashant.brain.service.RobotStateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/status")
@RequiredArgsConstructor
@Slf4j
public class StatusController {

    private final RobotStateService robotStateService;

    /**
     * GET endpoint to fetch current system info (all states + intent mode)
     */
    @GetMapping("/systeminfo")
    public Map<String, Object> getSystemInfo() {
        Map<String, Object> systemInfo = new HashMap<>();
        systemInfo.put("navigationState", robotStateService.getNavigationState());
        systemInfo.put("speechState", robotStateService.getSpeechState());
        systemInfo.put("visionState", robotStateService.getVisionState());
        systemInfo.put("intentMode", robotStateService.getIntendedMode());
        systemInfo.put("safeToMove", robotStateService.isSafeToMove());
        return systemInfo;
    }

    /**
     * POST endpoint to set states individually for testing
     */
    @PostMapping("/setstate")
    public String setState(@RequestParam(required = false) NavState navState,
                           @RequestParam(required = false) SpeechState speechState,
                           @RequestParam(required = false) VisionState visionState,
                           @RequestParam(required = false) IntentMode intentMode) {
        if (navState != null) {
            robotStateService.updateNavigationState(navState);
            log.info("Navigation state set to {}", navState);
        }
        if (speechState != null) {
            robotStateService.updateSpeechState(speechState);
            log.info("Speech state set to {}", speechState);
        }
        if (visionState != null) {
            robotStateService.updateVisionState(visionState);
            log.info("Vision state set to {}", visionState);
        }
        if (intentMode != null) {
            robotStateService.setIntendedMode(intentMode);
            log.info("Intent mode set to {}", intentMode);
        }
        return "States updated successfully";
    }
}
