package com.prashant.brain.service;

import com.prashant.brain.dto.*;
import com.prashant.brain.model.IntentMode;
import com.prashant.brain.model.NavState;
import com.prashant.brain.model.SpeechState;
import com.prashant.brain.model.VisionState;
import com.prashant.brain.kafka.CommandProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskOrchestratorService {

    private final RobotStateService robotStateService;
    private final FamilyProfileService familyProfileService;
    private final CommandProducer commandProducer;
    private final IntentParserService intentParserService;
    private final ObjectResponse objectResponse;

    public TaskOrchestratorService(
            RobotStateService robotStateService,
            FamilyProfileService familyProfileService,
            CommandProducer commandProducer,
            IntentParserService intentParserService,
            ObjectResponse objectResponse) {

        this.robotStateService = robotStateService;
        this.familyProfileService = familyProfileService;
        this.commandProducer = commandProducer;
        this.intentParserService = intentParserService;
        this.objectResponse = objectResponse;
    }

    // -------------------------------------------------------------------------
    // SPEECH EVENTS
    // -------------------------------------------------------------------------

    public void processSpeechEvent(SpeechEvent speechEvent) {

        if (speechEvent == null || speechEvent.getEvent() == null) {
            log.warn("Received null or invalid SpeechEvent: {}", speechEvent);
            return;
        }

        try {
            switch (speechEvent.getEvent()) {

                case "wake_word_detected":

                    robotStateService.updateSpeechState(SpeechState.LISTENING);

                    // Stop navigation when the robot is actively exploring.
                    // Preserve WANDER as the intended mode so navigation
                    // can resume after the interaction is completed.
                    if (robotStateService.getNavigationState()
                            == NavState.EXPLORING) {

                        robotStateService.updateNavigationState(
                                NavState.PAUSED_SPEECH
                        );

                        commandProducer.sendCommand(
                                new BrainCommand("navigation", "Stop", null)
                        );

                        log.info(
                                "Navigation paused because wake word was detected."
                        );
                    }

                    break;

                case "person_identified":

                    robotStateService.updateSpeechState(SpeechState.IDLE);

                    String name = speechEvent.getText();

                    familyProfileService.saveProfile(
                            name,
                            "Hello " + name + ", nice to meet you!"
                    );

                    robotStateService.updateVisionState(VisionState.SAVING);

                    commandProducer.sendCommand(
                            new BrainCommand("vision", "saveFace", name)
                    );

                    break;

                case "transcription_ready":

                    robotStateService.updateSpeechState(SpeechState.THINKING);

                    ParsedIntent parsedIntent =
                            intentParserService.parseIntent(
                                    speechEvent.getText()
                            );

                    if (parsedIntent == null) {
                        log.error(
                                "Failed to parse intent from speech text: {}",
                                speechEvent.getText()
                        );

                        robotStateService.updateSpeechState(SpeechState.ERROR);
                        return;
                    }

                    handleParsedIntent(parsedIntent);
                    break;

                default:

                    log.warn(
                            "Unhandled SpeechEvent type: {}",
                            speechEvent.getEvent()
                    );
            }

        } catch (Exception e) {

            log.error(
                    "Error processing SpeechEvent: {}",
                    speechEvent,
                    e
            );

            robotStateService.updateSpeechState(SpeechState.ERROR);
        }
    }

    private void handleParsedIntent(ParsedIntent parsedIntent) {

        try {
            switch (parsedIntent.getIntentCategory()) {

                case "speech":

                    commandProducer.sendCommand(
                            new BrainCommand(
                                    "speech",
                                    "speak",
                                    parsedIntent.getSpeechResponse()
                            )
                    );

                    robotStateService.updateSpeechState(SpeechState.SPEAKING);
                    break;

                case "navigation":

                    String action = parsedIntent.getCommandAction();

                    BrainCommand navCommand = new BrainCommand(
                            "navigation",
                            action,
                            parsedIntent.getSpeechResponse()
                    );

                    if ("Stop".equalsIgnoreCase(action)) {

                        robotStateService.updateNavigationState(
                                NavState.IDLE
                        );

                        robotStateService.setIntendedMode(
                                IntentMode.IDLE
                        );

                    } else if ("start".equalsIgnoreCase(action)) {

                        robotStateService.updateNavigationState(
                                NavState.EXPLORING
                        );

                        robotStateService.setIntendedMode(
                                IntentMode.WANDER
                        );
                    }

                    commandProducer.sendCommand(navCommand);

                    robotStateService.updateSpeechState(SpeechState.IDLE);
                    break;

                case "vision":

                    commandProducer.sendCommand(
                            new BrainCommand(
                                    "vision",
                                    parsedIntent.getCommandAction(),
                                    parsedIntent.getSpeechResponse()
                            )
                    );

                    robotStateService.updateVisionState(
                            VisionState.SCANNING
                    );

                    robotStateService.updateSpeechState(SpeechState.IDLE);
                    break;

                default:

                    log.warn(
                            "Unknown intent category: {}",
                            parsedIntent.getIntentCategory()
                    );

                    robotStateService.updateSpeechState(SpeechState.IDLE);
            }

        } catch (Exception e) {

            log.error(
                    "Error handling ParsedIntent: {}",
                    parsedIntent,
                    e
            );

            robotStateService.updateSpeechState(SpeechState.ERROR);
        }
    }

    // -------------------------------------------------------------------------
    // NAVIGATION EVENTS
    // -------------------------------------------------------------------------

    public void processNavigationEvent(NavEvent navEvent) {

        if (navEvent == null || navEvent.getEvent() == null) {
            log.warn("Received null or invalid NavEvent: {}", navEvent);
            return;
        }

        try {
            switch (navEvent.getEvent()) {

                case "obstacle_detected":

                    // Mark navigation as blocked.
                    robotStateService.updateNavigationState(
                            NavState.BLOCKED
                    );

                    // Explicitly stop navigation before requesting a scan.
                    commandProducer.sendCommand(
                            new BrainCommand("navigation", "Stop", null)
                    );

                    // Request Vision to inspect the surroundings.
                    robotStateService.updateVisionState(
                            VisionState.SCANNING
                    );

                    commandProducer.sendCommand(
                            new BrainCommand("vision", "scan", null)
                    );

                    log.info(
                            "Obstacle detected. Navigation stopped and Vision scan requested."
                    );

                    break;

                default:

                    log.warn(
                            "Unhandled NavEvent type: {}",
                            navEvent.getEvent()
                    );
            }

        } catch (Exception e) {

            log.error(
                    "Error processing NavEvent: {}",
                    navEvent,
                    e
            );

            robotStateService.updateNavigationState(NavState.IDLE);
        }
    }

    // -------------------------------------------------------------------------
    // VISION EVENTS
    // -------------------------------------------------------------------------

    public void processVisionEvent(VisionEvent visionEvent) {

        if (visionEvent == null || visionEvent.getEvent() == null) {
            log.warn("Received null or invalid VisionEvent: {}", visionEvent);
            return;
        }

        try {
            switch (visionEvent.getEvent()) {

                case "unknown_person_detected":

                    robotStateService.updateVisionState(VisionState.IDLE);

                    commandProducer.sendCommand(
                            new BrainCommand(
                                    "speech",
                                    "identify",
                                    "I see someone I don't recognize. Please identify yourself."
                            )
                    );

                    robotStateService.updateSpeechState(
                            SpeechState.LISTENING
                    );

                    break;

                case "person_detected":

                    robotStateService.updateVisionState(VisionState.IDLE);

                    String greeting =
                            familyProfileService.getGreetingForPerson(
                                    visionEvent.getName()
                            );

                    commandProducer.sendCommand(
                            new BrainCommand("speech", "speak", greeting)
                    );

                    robotStateService.updateSpeechState(
                            SpeechState.SPEAKING
                    );

                    break;

                case "object_detected":

                    robotStateService.updateVisionState(VisionState.IDLE);

                    String reaction =
                            objectResponse.GenerateReaction(
                                    visionEvent.getObjectName()
                            );

                    commandProducer.sendCommand(
                            new BrainCommand("speech", "speak", reaction)
                    );

                    robotStateService.updateSpeechState(
                            SpeechState.SPEAKING
                    );

                    break;

                case "saved_face":

                    robotStateService.updateVisionState(VisionState.IDLE);

                    String savedGreeting =
                            familyProfileService.getGreetingForPerson(
                                    visionEvent.getName()
                            );

                    commandProducer.sendCommand(
                            new BrainCommand(
                                    "speech",
                                    "speak",
                                    savedGreeting
                            )
                    );

                    robotStateService.updateSpeechState(
                            SpeechState.SPEAKING
                    );

                    break;

                case "no_face_detected":

                    robotStateService.updateVisionState(VisionState.IDLE);

                    commandProducer.sendCommand(
                            new BrainCommand(
                                    "speech",
                                    "speak",
                                    "I don't see anyone around."
                            )
                    );

                    robotStateService.updateSpeechState(
                            SpeechState.SPEAKING
                    );

                    break;

                default:

                    log.warn(
                            "Unhandled VisionEvent type: {}",
                            visionEvent.getEvent()
                    );
            }

        } catch (Exception e) {

            log.error(
                    "Error processing VisionEvent: {}",
                    visionEvent,
                    e
            );

            robotStateService.updateVisionState(VisionState.IDLE);
        }
    }

    // -------------------------------------------------------------------------
    // TASK COMPLETION EVENTS
    // -------------------------------------------------------------------------

    public void processTaskCompletion(TaskCompletion taskCompletion) {

        if (taskCompletion == null || taskCompletion.getTask() == null) {
            log.warn(
                    "Received null or invalid TaskCompletion: {}",
                    taskCompletion
            );
            return;
        }

        try {
            switch (taskCompletion.getTask().toLowerCase()) {

                case "speak":

                    robotStateService.updateSpeechState(SpeechState.IDLE);

                    // Resume wandering only when:
                    // 1. The intended mode is still WANDER.
                    // 2. Navigation was paused for speech or blocked by an obstacle.
                    // 3. Speech and Vision are no longer busy.
                    if (robotStateService.getIntendedMode()
                            == IntentMode.WANDER

                            && (robotStateService.getNavigationState()
                            == NavState.PAUSED_SPEECH

                            || robotStateService.getNavigationState()
                            == NavState.BLOCKED)

                            && robotStateService.isSafeToMove()) {

                        commandProducer.sendCommand(
                                new BrainCommand(
                                        "navigation",
                                        "start",
                                        null
                                )
                        );

                        robotStateService.updateNavigationState(
                                NavState.EXPLORING
                        );

                        log.info(
                                "Speech completed. Resuming navigation because intended mode is WANDER and the robot is safe to move."
                        );

                    } else {

                        log.info(
                                "Speech completed. Navigation will not resume. Intended mode={}, navigation state={}, safe to move={}",
                                robotStateService.getIntendedMode(),
                                robotStateService.getNavigationState(),
                                robotStateService.isSafeToMove()
                        );
                    }

                    break;

                default:

                    log.warn(
                            "Unhandled TaskCompletion type: {}",
                            taskCompletion.getTask()
                    );
            }

        } catch (Exception e) {

            log.error(
                    "Error processing TaskCompletion: {}",
                    taskCompletion,
                    e
            );

            robotStateService.updateSpeechState(SpeechState.IDLE);
        }
    }
}