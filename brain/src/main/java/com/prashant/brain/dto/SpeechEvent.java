package com.prashant.brain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SpeechEvent {
    String event;
    String text;
    String timestamp;
}
