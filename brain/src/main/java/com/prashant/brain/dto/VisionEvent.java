package com.prashant.brain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VisionEvent {
    String event;
    String name;
    Double confidence;
    String objectName;
    String timestamp;

}
