package com.prashant.brain.kafka;

import com.prashant.brain.dto.NavEvent;
import com.prashant.brain.dto.SpeechEvent;
import com.prashant.brain.dto.TaskCompletion;
import com.prashant.brain.dto.VisionEvent;
import com.prashant.brain.service.TaskOrchestratorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
public class EventConsumer {

    private final JsonMapper jsonMapper;
    private final TaskOrchestratorService taskOrchestratorService;
    public EventConsumer(JsonMapper jsonMapper,
                         TaskOrchestratorService taskOrchestratorService) {
        this.jsonMapper = jsonMapper;
        this.taskOrchestratorService = taskOrchestratorService;

    }

    @KafkaListener(topics = "${robot.kafka.topics.nav-events}", groupId = "${spring.kafka.consumer.group-id:}")
    public void consumeNavEvent(String message) {
        try {
            NavEvent navEvent = jsonMapper.readValue(message, NavEvent.class);
            handleNavEvent(navEvent);
            log.info("Parsed nav event: {}", navEvent);
        } catch (Exception e) {
            log.error("Failed to parse nav event", e);
        }
    }

    @KafkaListener(topics = "${robot.kafka.topics.vision-events}", groupId = "${spring.kafka.consumer.group-id:}")
    public void consumeVisionEvent(String message) {
        try {
            VisionEvent visionEvent = jsonMapper.readValue(message, VisionEvent.class);
            handleVisionEvent(visionEvent);
            log.info("Parsed vision event: {}", visionEvent);
        } catch (Exception e) {
            log.error("Failed to parse vision event", e);
        }
    }

    @KafkaListener(topics = "${robot.kafka.topics.speech-events}", groupId = "${spring.kafka.consumer.group-id:}")
    public void consumeSpeechEvent(String message) {
        log.info("RAW SPEECH EVENT RECEIVED FROM KAFKA: {}", message);
        try {
            SpeechEvent speechEvent = jsonMapper.readValue(message, SpeechEvent.class);
            handleSpeechEvent(speechEvent);
            log.info("Parsed speech event: {}", speechEvent);
        } catch (Exception e) {
            log.error("Failed to parse speech event", e);
        }
    }

    @KafkaListener(topics = "${robot.kafka.topics.task-completions}", groupId = "${spring.kafka.consumer.group-id:}")
    public void consumeTaskCompletion(String message) {
        try {
            TaskCompletion taskCompletion = jsonMapper.readValue(message, TaskCompletion.class);
            handleTaskCompletion(taskCompletion);
            log.info("Parsed task completion: {}", taskCompletion);
        } catch (Exception e) {
            log.error("Failed to parse task completion", e);
        }
    }

    private void handleNavEvent(NavEvent navEvent) {
        taskOrchestratorService.processNavigationEvent(navEvent);
    }

    private void handleVisionEvent(VisionEvent visionEvent) {
        taskOrchestratorService.processVisionEvent(visionEvent);
    }

    private void handleSpeechEvent(SpeechEvent speechEvent) {
        taskOrchestratorService.processSpeechEvent(speechEvent);
    }

    private void handleTaskCompletion(TaskCompletion taskCompletion) {
        taskOrchestratorService.processTaskCompletion(taskCompletion);
    }
}
