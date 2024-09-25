package com.aws.rulesengine.dynamicrules.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.JexlContext;

import com.aws.rulesengine.dynamicrules.objects.SensorEvent;
import com.aws.rulesengine.dynamicrules.objects.SensorMapState;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AlertUtils {

    public static List<SensorEvent> alertMapToSensorEventList(JexlContext mapContext, Set<String> sensorIds) {
        List<SensorEvent> sensorEvents = new ArrayList<>();
        sensorIds.forEach(sensorId -> {
            SensorMapState sensorMapState = (SensorMapState) mapContext.get(sensorId);
            sensorEvents.addAll(sensorMapState.getSensorEvents().values());
        });
        return sensorEvents;
    }
}
