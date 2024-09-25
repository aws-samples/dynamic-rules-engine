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

import java.util.Map;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.aws.rulesengine.dynamicrules.objects.Rule;
import com.aws.rulesengine.dynamicrules.objects.SensorEvent;
import com.aws.rulesengine.dynamicrules.utils.Descriptors;
import com.aws.rulesengine.dynamicrules.utils.Keyed;
import com.aws.rulesengine.dynamicrules.utils.ProcessingUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicKeyFunction
        extends BroadcastProcessFunction<SensorEvent, Rule, Keyed<SensorEvent, String, String>> {

    @Override
    public void processElement(
            SensorEvent event, ReadOnlyContext ctx, Collector<Keyed<SensorEvent, String, String>> out)
            throws Exception {
        ReadOnlyBroadcastState<String, Rule> rulesState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
        // We want to fork the event for each rule that contains the sensorId of the event
        forkEventForEachGroupingKey(event, rulesState, out);
    }

    private void forkEventForEachGroupingKey(
            SensorEvent event,
            ReadOnlyBroadcastState<String, Rule> rulesState,
            Collector<Keyed<SensorEvent, String, String>> out)
            throws Exception {
        for (Map.Entry<String, Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue();
            if (rule.getSensorWindowMap().containsKey(event.getId())) {
                String key = rule.getId() + "_" + event.getEquipment().getId();
                log.debug("Found sensor event {} for rule {}", event.getId(), rule.getName());
                out.collect(
                        new Keyed<>(
                                event, key, rule.getId()));
            }
        }
    }

    @Override
    public void processBroadcastElement(
        Rule rule, Context ctx, Collector<Keyed<SensorEvent, String, String>> out) throws Exception {
        BroadcastState<String, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
        ProcessingUtils.handleRuleBroadcast(rule, broadcastState);
    }
}
