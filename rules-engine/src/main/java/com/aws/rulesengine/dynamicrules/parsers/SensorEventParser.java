package com.aws.rulesengine.dynamicrules.parsers;

import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.aws.rulesengine.dynamicrules.objects.SensorEvent;

public class SensorEventParser {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SensorEvent fromString(String line) throws IOException {
        return parseJson(line);
    }

    private SensorEvent parseJson(String ruleString) throws IOException {
        return objectMapper.readValue(ruleString, SensorEvent.class);
    }
}
