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

package com.aws.rulesengine.dynamicrules.utils;

import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

import com.aws.rulesengine.dynamicrules.objects.Rule;
import com.aws.rulesengine.dynamicrules.objects.SensorEvent;
import com.aws.rulesengine.dynamicrules.objects.SensorMapState;

public class ProcessingUtils {

    public static void handleRuleBroadcast(Rule rule, BroadcastState<String, Rule> broadcastState)
            throws Exception {
        switch (rule.getStatus()) {
            case ACTIVE:
                broadcastState.put(rule.getId(), rule);
                break;
            case INACTIVE:
                broadcastState.remove(rule.getId());
                break;
        }
    }

    public static void addToWindow(MapState<String, SensorMapState> mapState, String key, SensorEvent value)
            throws Exception {

        SensorMapState sensorMapState = mapState.get(key);

        if (sensorMapState == null) {
            sensorMapState = new SensorMapState();
            sensorMapState.addSensorEvent(value);
            mapState.put(key, sensorMapState);
        } else {
            sensorMapState.addSensorEvent(value);
        }
    }

    public static <K, V> void clearMapState(MapState<K, V> mapState) {
        try {
            Iterator<Map.Entry<K, V>> mapStateKV = mapState.iterator();
            while (mapStateKV.hasNext()) {
                mapStateKV.next();
                mapStateKV.remove();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Rule updateRule(ValueState<Rule> valueState, Rule rule)
            throws Exception {
        valueState.update(rule);
        return valueState.value();
    }


}
