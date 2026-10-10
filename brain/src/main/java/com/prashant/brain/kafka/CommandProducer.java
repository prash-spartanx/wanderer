package com.prashant.brain.kafka;

import com.prashant.brain.dto.BrainCommand;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
public class CommandProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    @Value("${robot.kafka.topics.brain-commands}")
    private String topicName;
    public CommandProducer(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }
    public void sendCommand(BrainCommand command){
        try{
            String command_str = jsonMapper.writeValueAsString(command);
            kafkaTemplate.send(topicName, command_str);
            log.info("Sent command to topic {}: {}", topicName, command_str);
        } catch (Exception e) {
            log.error("Failed to send command to topic {}: {}", topicName, e.getMessage());
        }
    }

}
