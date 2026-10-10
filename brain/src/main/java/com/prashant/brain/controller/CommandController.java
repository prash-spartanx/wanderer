package com.prashant.brain.controller;

import com.prashant.brain.dto.BrainCommand;
import com.prashant.brain.kafka.CommandProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/commands")
@RequiredArgsConstructor
@Slf4j
public class CommandController {

    private final CommandProducer commandProducer;

    @Value("${robot.kafka.topics.nav-events}")
    private String navEventTopic;

    @Value("${robot.kafka.topics.vision-events}")
    private String visionEventTopic;

    @Value("${robot.kafka.topics.speech-events}")
    private String speechEventTopic;

    @Value("${robot.kafka.topics.task-completions}")
    private String taskCompletionTopic;

    /**
     * Send a manual navigation command
     */
    @PostMapping("/navigation")
    public String sendNavigationCommand(@RequestParam String action,
                                        @RequestParam(required = false) String payload) {
        BrainCommand command = new BrainCommand(navEventTopic, action, payload);
        commandProducer.sendCommand(command);
        log.info("Manual navigation command sent: {}", command);
        return "Navigation command sent: " + action;
    }

    /**
     * Send a manual vision command
     */
    @PostMapping("/vision")
    public String sendVisionCommand(@RequestParam String action,
                                    @RequestParam(required = false) String payload) {
        BrainCommand command = new BrainCommand(visionEventTopic, action, payload);
        commandProducer.sendCommand(command);
        log.info("Manual vision command sent: {}", command);
        return "Vision command sent: " + action;
    }

    /**
     * Send a manual speech command
     */
    @PostMapping("/speech")
    public String sendSpeechCommand(@RequestParam String action,
                                    @RequestParam(required = false) String payload) {
        BrainCommand command = new BrainCommand(speechEventTopic, action, payload);
        commandProducer.sendCommand(command);
        log.info("Manual speech command sent: {}", command);
        return "Speech command sent: " + action;
    }

    /**
     * Send a manual task completion command
     */
    @PostMapping("/task")
    public String sendTaskCompletionCommand(@RequestParam String action,
                                            @RequestParam(required = false) String payload) {
        BrainCommand command = new BrainCommand(taskCompletionTopic, action, payload);
        commandProducer.sendCommand(command);
        log.info("Manual task completion command sent: {}", command);
        return "Task completion command sent: " + action;
    }
}
