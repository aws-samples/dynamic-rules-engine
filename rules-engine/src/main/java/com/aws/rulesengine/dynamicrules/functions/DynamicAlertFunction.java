/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aws.rulesengine.dynamicrules.functions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.aws.rulesengine.dynamicrules.exceptions.InsufficientDataException;
import com.aws.rulesengine.dynamicrules.exceptions.RuleNotFoundException;
import com.aws.rulesengine.dynamicrules.objects.Alert;
import com.aws.rulesengine.dynamicrules.objects.AlertStatus;
import com.aws.rulesengine.dynamicrules.objects.OperationStatus;
import com.aws.rulesengine.dynamicrules.objects.Rule;
import com.aws.rulesengine.dynamicrules.objects.RuleOperationStatus;
import com.aws.rulesengine.dynamicrules.objects.SensorEvent;
import com.aws.rulesengine.dynamicrules.objects.SensorMapState;
import com.aws.rulesengine.dynamicrules.objects.Status;
import com.aws.rulesengine.dynamicrules.utils.AlertUtils;
import com.aws.rulesengine.dynamicrules.utils.Descriptors;
import com.aws.rulesengine.dynamicrules.utils.Keyed;
import com.aws.rulesengine.dynamicrules.utils.ProcessingUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Implements main rule evaluation and alerting logic.
 */
@Slf4j
public class DynamicAlertFunction
        extends KeyedBroadcastProcessFunction<String, Keyed<SensorEvent, String, String>, Rule, Alert> {

    private static final JexlEngine jexl = new JexlBuilder()
            .permissions(JexlPermissions.RESTRICTED.compose("com.aws.rulesengine.dynamicrules.*")).cache(512)
            .strict(true).silent(false)
            .create();
    private final MapStateDescriptor<String, SensorMapState> mapStateDescriptor = new MapStateDescriptor<>("mapState",
            BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<SensorMapState>() {
            }));
    private final ValueStateDescriptor<Rule> ruleStateDescriptor = new ValueStateDescriptor<>("ruleState",
            TypeInformation.of(new TypeHint<Rule>() {
            }));
    private final ValueStateDescriptor<Alert> alertStateDescriptor = new ValueStateDescriptor<>("alertState",
            TypeInformation.of(new TypeHint<Alert>() {
            }));

    private transient ValueState<Rule> latestRuleValue;
    private transient MapState<String, SensorMapState> sensorWindow;
    private transient ValueState<Alert> lastAlertState;

    @Override
    public void open(Configuration parameters) {
        // Create States to persist info
        sensorWindow = getRuntimeContext().getMapState(mapStateDescriptor);
        latestRuleValue = getRuntimeContext().getState(ruleStateDescriptor);
        lastAlertState = getRuntimeContext().getState(alertStateDescriptor);
    }

    @Override
    public void processElement(
            Keyed<SensorEvent, String, String> value, ReadOnlyContext ctx, Collector<Alert> out)
            throws Exception {

        SensorEvent sensorEvent = value.getWrapped();
        String sensorId = sensorEvent.getId();

        log.debug("Processing event for sensorId {} with rule {}", sensorId, value.getId());

        // Clean up old events
        long currentEventTime = value.getWrapped().getEventTimestamp();
        log.debug("Current event time {} vs timestamp {}", currentEventTime, ctx.timestamp());

        Long currentEvalTime = System.currentTimeMillis();

        ProcessingUtils.addToWindow(sensorWindow, sensorId, value.getWrapped());

        Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());

        ProcessingUtils.updateRule(latestRuleValue, rule);

        // Evaluate rule
        evaluateRule(currentEvalTime, rule, ctx, out, "ProcessElement");

        // Schedule re-evaluation after the minimium sensor window time
        ctx.timerService().registerProcessingTimeTimer(getReevaluationTime(rule));
    }

    private void evaluateRule(Long currentEvalTime, Rule rule, ReadOnlyContext ctx,
            Collector<Alert> out, String source) throws Exception {
        try {
            log.debug("Evaluating rule {} from {}", rule.getId(), source);
            if (isRuleValidForEvaluation(rule)) {
                JexlContext context = new MapContext();

                // Check and make sure we have all the necessary data in the context window
                for (String ruleSensorId : rule.getSensorWindowMap().keySet()) {
                    if (!sensorWindow.contains(ruleSensorId)) {
                        // Here is where we'll send an alert that we have insufficient data
                        log.debug("DynamicAlertFunction - SensorId " + ruleSensorId + " not found");
                        throw new InsufficientDataException(rule);
                    }
                    context.set(ruleSensorId, sensorWindow.get(ruleSensorId));
                }
                // Rule Evaluation Logic
                JexlExpression expression = jexl.createExpression(rule.getRuleExpression());
                Boolean isAlertTriggered = (Boolean) expression.evaluate(context);

                // Logic to handle the creation & triggering of an alert
                handleAlert(isAlertTriggered, currentEvalTime, rule, context, out);

                // Update the status of the rule that is was successfully evaluated (true or
                // false doesn't matter for this status)
                outputRuleOpData(rule, OperationStatus.SUCCESS, currentEvalTime, ctx);
            }
        } catch (RuleNotFoundException e) {
            log.error("Error while evaluating rule", e);
        } catch (InsufficientDataException e) {
            log.debug("Insufficient data for rule evaluation");
            outputRuleOpData(rule, OperationStatus.INSUFFICIENT_DATA, currentEvalTime, ctx);
        } catch (JexlException e) {
            log.error("Error while evaluating JEXL expression", e);
            outputRuleOpData(rule, OperationStatus.FAILURE, currentEvalTime, e.getMessage(), ctx);
        }
    }

    private boolean isRuleValidForEvaluation(Rule rule) {
        if (rule == null) {
            log.error("Rule does not exist");
            return false;
        }
        if (rule.getStatus() != Status.ACTIVE) {
            log.debug("Rule {} is not active, skipping evaluation", rule.getId());
            return false;
        }
        return true;
    }

    // This is our stream of the rule operations status that we use to update the
    // rule operation dashboard
    // so that we know the latest state of the rule
    private void outputRuleOpData(Rule rule, OperationStatus status, Long currentEventTime, ReadOnlyContext ctx) {
        outputRuleOpData(rule, status, currentEventTime, null, ctx);
    }

    private void outputRuleOpData(Rule rule, OperationStatus status, Long currentEventTime, String message,
            ReadOnlyContext ctx) {
        RuleOperationStatus ruleOperationStatus = new RuleOperationStatus(
                rule.getEquipmentName(),
                rule.getName(),
                rule.getId(),
                status,
                currentEventTime, message);
        log.debug("Rule Operation Update - {}", ruleOperationStatus.getStatus());
        ctx.output(Descriptors.ruleOperations, ruleOperationStatus);
    }

    private void outputRuleOpData(Rule rule, OperationStatus status, Long currentEventTime, Context ctx) {
        RuleOperationStatus ruleOperationStatus = new RuleOperationStatus(
                rule.getEquipmentName(),
                rule.getName(),
                rule.getId(),
                status,
                currentEventTime, null);
        log.debug("Rule Operation Update - {}", ruleOperationStatus.getStatus());
        ctx.output(Descriptors.ruleOperations, ruleOperationStatus);
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
            throws Exception {
        BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
        Long currentProcessTime = System.currentTimeMillis();
        // If we get a new rule, we'll give it insufficient data rule op status
        if (!broadcastState.contains(rule.getId())) {
            outputRuleOpData(rule, OperationStatus.INSUFFICIENT_DATA, currentProcessTime, ctx);
        }
        ProcessingUtils.handleRuleBroadcast(rule, broadcastState);
    }

    @Override
    public void onTimer(final long timestamp,
            final KeyedBroadcastProcessFunction<String, Keyed<SensorEvent, String, String>, Rule, Alert>.OnTimerContext ctx,
            final Collector<Alert> out)
            throws Exception {

        // Get the applicable rule
        Rule rule = latestRuleValue.value();

        // Clean up state
        if (rule.getStatus() == Status.INACTIVE) {
            ProcessingUtils.clearMapState(sensorWindow);
        }
        // This timer will allow us to reevaluate the rule
        // But first we should make sure we get rid of the outdated data
        evictAgedElementsFromWindow(timestamp, rule);
        evaluateRule(timestamp, rule, ctx, out, "onTimer");

    }

    private void evictAgedElementsFromWindow(final long timestamp, Rule rule) {
        try {
            Iterator<Map.Entry<String, SensorMapState>> windowStateKV = sensorWindow.iterator();
            // Iterate over the map and remove the entries that are older than the sensor
            // window
            while (windowStateKV.hasNext()) {
                Map.Entry<String, SensorMapState> kv = windowStateKV.next();
                Integer sensorWindow = rule.getSensorWindowMap().getOrDefault(kv.getKey(), 15);
                long timeToEvict = timestamp - (sensorWindow * 60000);
                log.debug("Rule {} Time to evict {} w/ sensor window {} for sensor_id {}", rule.getName(), timeToEvict,
                        sensorWindow, kv.getKey());
                kv.getValue().removeOlderThan(timeToEvict);
                if (kv.getValue().hasNoEvents()) {
                    windowStateKV.remove();
                    log.debug("SensorId {} has no events", kv.getKey());
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // Returns time of when to reevaluate the rule again (for timebased rules)
    private long getReevaluationTime(Rule rule) {
        Integer minSensorWindow = rule.getSensorWindowMap().values().stream().min(Integer::compare).orElse(15);
        return System.currentTimeMillis() + (minSensorWindow * 60000);
    }

    private void handleAlert(Boolean isAlertTriggered, Long currentEvalTime, Rule rule, JexlContext context,
            Collector<Alert> out) throws IOException {

        // Create triggering events
        List<SensorEvent> triggeringEvents = isAlertTriggered
                ? AlertUtils.alertMapToSensorEventList(context, rule.getSensorWindowMap().keySet())
                : Collections.emptyList();

        // Create alert
        Alert alert = new Alert(rule.getEquipmentName(), rule.getName(), rule.getId(),
                isAlertTriggered ? AlertStatus.START : AlertStatus.STOP,
                triggeringEvents, currentEvalTime);

        // Determine if an alert should be emitted
        boolean shouldEmitAlert = false;

        if (lastAlertState.value() == null) {
            // If there's no previous alert state, emit the alert if it's a START alert
            // Doesn't make sense to emit a 'STOP' state first
            shouldEmitAlert = (alert.getStatus() == AlertStatus.START);
        } else {
            // If there's a previous alert state, emit the alert if the status has changed
            // If we transition from START -> STOP or vice versa
            shouldEmitAlert = (lastAlertState.value().getStatus() != alert.getStatus());
        }
        if (shouldEmitAlert) {
            log.debug("Pushing {} alert for {}", alert.getStatus(), rule.getName());
            out.collect(alert);
            lastAlertState.update(alert);
        }
        out.collect(alert);
    }

}
