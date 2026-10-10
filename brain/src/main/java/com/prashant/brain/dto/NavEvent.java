package com.prashant.brain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NavEvent {
    String event;
    @JsonProperty("distance_cm")
    Double distanceCm;
    Double confidence;
    String timestamp;
}
