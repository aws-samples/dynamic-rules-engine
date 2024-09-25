package com.aws.rulesengine.dynamicrules.objects;

import java.util.Set;
import java.util.TreeMap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@EqualsAndHashCode
@ToString
@Data
@Slf4j
public class SensorMapState {
    @NonNull
    @Singular
    private TreeMap<Long, SensorEvent> sensorEvents;

    public SensorMapState() {
        sensorEvents = new TreeMap<>();
    }

    public void addSensorEvent(SensorEvent sensorEvent) {
        log.debug("Adding sensorEvent {} w/ time {}", sensorEvent.getId(), sensorEvent.getEventTimestamp());
        sensorEvents.put(sensorEvent.getEventTimestamp(), sensorEvent);
    }

    public void removeOlderThan(Long timestamp) {
        Set<Long> keys = sensorEvents.keySet();
        for (Long key : keys) {
            if (key < timestamp) {
                log.debug("Removing sensorEvent w/ timestamp {}; older than {}", key, timestamp);
                sensorEvents.remove(key);
            }
        }
    }

    public boolean hasNoEvents() {
        return sensorEvents.isEmpty();
    }

    public Double getValue(){
    	if (hasNoEvents()) {
            return 0.0;
        }
        SensorEvent sensorEvent = sensorEvents.lastEntry().getValue();
        return sensorEvent.getMeasureValue();
    }

    public Boolean isValueBetween(Double start, Double end){
        if (hasNoEvents()) {
            return false;
        }
        SensorEvent sensorEvent = sensorEvents.lastEntry().getValue();
        return sensorEvent.getMeasureValue() >= start && sensorEvent.getMeasureValue() <= end;
    }

    public Long getMinutesSinceChange() {
        if (hasNoEvents()) {
            return 0L;
        }
        Double initialSensorValue = sensorEvents.lastEntry().getValue().getMeasureValue();
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(minutesSinceValueChange).getMeasureValue();
            if (initialSensorValue != sensorValue) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Boolean hasChanged(Integer time) {
        Long minutesSinceChange = getMinutesSinceChange();
        log.debug("Time: " + time + " | Minutes since change: " + minutesSinceChange);
        return minutesSinceChange <= time;
    }

    public Boolean hasNotChanged(Integer time) {
        Long minutesSinceChange = getMinutesSinceChange();
        log.debug("Time: " + time + " | Minutes since change: " + minutesSinceChange);
        return minutesSinceChange >  time;
    }

    public Long getMinutesSinceComparison(Double compareValue, String operator) {
        if(operator.equals(">")) {
            return getMinutesSinceValueGreaterThan(compareValue);
        }
        else if (operator.equals(">=")) {
            return getMinutesSinceValueGreaterEqualThan(compareValue);
        }
        else if (operator.equals("<")) {
            return getMinutesSinceValueLessThan(compareValue);
        }
        else if (operator.equals("<=")) {
            return getMinutesSinceValueLessEqualThan(compareValue);
        }
        else if (operator.equals("==")) {
            return getMinutesSinceValueEquals(compareValue);
        }
        else if (operator.equals("!=")) {
            return getMinutesSinceValueNotEquals(compareValue);
        }
        return 0L;
    }

    public Long getMinutesSinceComparison(Integer compareValueInt, String operator) {
        Double compareValue = compareValueInt.doubleValue();
        return getMinutesSinceComparison(compareValue,operator);
    }

    public Long getMinutesSinceValueGreaterThan(Double greaterThan) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue <= greaterThan) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueGreaterEqualThan(Double greaterThan) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue < greaterThan) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueLessThan(Double lessThan) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue >= lessThan) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueLessEqualThan(Double lessThan) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue > lessThan) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueEquals(Double equals) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue != equals) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueNotEquals(Double notEquals) {
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue == notEquals) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }

    public Long getMinutesSinceValueBetween(Double start, Double end) {
        if (start == end) {
            return 0L;
        }
        Long minutesSinceValueChange = 0L;
        for (Long mapKey : sensorEvents.descendingKeySet()) {
            minutesSinceValueChange = mapKey;
            Double sensorValue = sensorEvents.get(mapKey).getMeasureValue();
            log.debug("MapKey: {} | SensorValue: {}", mapKey, sensorValue);
            // if sensorValue is NOT between start and end, then return the time since the first sensor event
            if (sensorValue < start || sensorValue > end) {
                return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
            }
        }
        return (System.currentTimeMillis() - minutesSinceValueChange) / 60000;
    }
}