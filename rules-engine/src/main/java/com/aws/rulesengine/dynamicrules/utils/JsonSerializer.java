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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonSerializer<T> extends RichFlatMapFunction<T, String> {

    private final Class<T> targetClass;
    private JsonMapper<T> parser;

    public JsonSerializer(Class<T> sourceClass) {
        this.targetClass = sourceClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parser = new JsonMapper<>(targetClass);
    }

    @Override
    public void flatMap(T value, Collector<String> out) throws Exception {
        System.out.println(value);
        try {
            String serialized = parser.toString(value);
            out.collect(serialized);
        } catch (Exception e) {
            log.warn("Failed serializing to JSON dropping it:", e);
        }
    }
}
