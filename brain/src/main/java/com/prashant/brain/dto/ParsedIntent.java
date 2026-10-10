package com.prashant.brain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParsedIntent {
    private String intentCategory;
    private String commandAction;
    private String speechResponse;
}
