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

package com.aws.rulesengine.dynamicrules;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import com.aws.rulesengine.dynamicrules.functions.DynamicAlertFunction;
import com.aws.rulesengine.dynamicrules.functions.DynamicKeyFunction;
import com.aws.rulesengine.dynamicrules.objects.Alert;
import com.aws.rulesengine.dynamicrules.objects.Rule;
import com.aws.rulesengine.dynamicrules.objects.RuleOperationStatus;
import com.aws.rulesengine.dynamicrules.objects.SensorEvent;
import com.aws.rulesengine.dynamicrules.utils.Descriptors;
import com.aws.rulesengine.dynamicrules.utils.JsonDeserializer;
import com.aws.rulesengine.dynamicrules.utils.JsonSerializer;
import com.aws.rulesengine.dynamicrules.utils.KinesisUtils;
import com.aws.rulesengine.dynamicrules.utils.RuleDeserializer;
import com.aws.rulesengine.dynamicrules.utils.TimeStamper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RulesEvaluator {

        RulesEvaluator() {
        }

        /**
         * The main method that sets up the Flink streaming pipeline for processing
         * sensor events,
         * evaluating rules, and generating alerts and rule operation status updates.
         *
         * @throws Exception if an error occurs during pipeline execution
         */
        public void run() throws Exception {
                // Environment setup
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // Source and Sink Properties setup
                Properties sourceProperties = createSourceProperties();
                Properties sinkProperties = createSinkProperties();

                // Streams setup
                DataStream<SensorEvent> sensorEvents = createSensorEventStream(env, sourceProperties);
                BroadcastStream<Rule> rulesStream = createRuleStream(env, sourceProperties)
                                .broadcast(Descriptors.rulesDescriptor);

                // Processing pipeline setup
                DataStream<Alert> alerts = sensorEvents
                                .connect(rulesStream)
                                .process(new DynamicKeyFunction())
                                .uid("partition-sensor-data")
                                .name("Partition Sensor Data by Equipment and RuleId")
                                .keyBy((equipmentSensorHash) -> equipmentSensorHash.getKey())
                                .connect(rulesStream)
                                .process(new DynamicAlertFunction())
                                .uid("rule-evaluator")
                                .name("Rule Evaluator");

                DataStream<RuleOperationStatus> ruleOperationsSink = ((SingleOutputStreamOperator<Alert>) alerts)
                                .getSideOutput(Descriptors.ruleOperations);

                alerts.flatMap(new JsonSerializer<>(Alert.class))
                                .name("Alerts Deserialization").sinkTo(createAlertSink(sinkProperties))
                                .uid("alerts-json-sink")
                                .name("Alerts JSON Sink");

                ruleOperationsSink
                                .flatMap(new JsonSerializer<>(RuleOperationStatus.class))
                                .name("Rule Operations Data Stream").sinkTo(createRuleOperationsSink(sinkProperties))
                                .uid("rule-operations-json-sink")
                                .name("Rule Operations JSON Sink");

                env.execute("Dynamic Rules Engine");
        }

        /**
         * Creates a DataStream of SensorEvent objects by consuming sensor event data
         * from a Kinesis stream.
         *
         * @param env The StreamExecutionEnvironment for the Flink job
         * @return A DataStream of SensorEvent objects
         * @throws IOException if an error occurs while reading Kinesis properties
         */
        private DataStream<SensorEvent> createSensorEventStream(StreamExecutionEnvironment env,
                        Properties sourceProperties) throws IOException {
                String DATA_SOURCE = KinesisUtils.getKinesisRuntimeProperty("kinesis", "dataTopicName");
                FlinkKinesisConsumer<String> kinesisConsumer = new FlinkKinesisConsumer<>(DATA_SOURCE,
                                new SimpleStringSchema(),
                                sourceProperties);
                DataStream<String> transactionsStringsStream = env.addSource(kinesisConsumer)
                                .name("EventStream")
                                .uid("sensor-events-stream");

                return transactionsStringsStream.flatMap(new JsonDeserializer<>(SensorEvent.class))
                                .returns(SensorEvent.class)
                                .flatMap(new TimeStamper<>())
                                .returns(SensorEvent.class)
                                .name("Transactions Deserialization");
        }

        /**
         * Creates a DataStream of Rule objects by consuming rule data from a Kinesis
         * stream.
         *
         * @param env The StreamExecutionEnvironment for the Flink job
         * @return A DataStream of Rule objects
         * @throws IOException if an error occurs while reading Kinesis properties
         */
        private DataStream<Rule> createRuleStream(StreamExecutionEnvironment env, Properties sourceProperties)
                        throws IOException {
                String RULES_SOURCE = KinesisUtils.getKinesisRuntimeProperty("kinesis", "rulesTopicName");
                FlinkKinesisConsumer<String> kinesisConsumer = new FlinkKinesisConsumer<>(RULES_SOURCE,
                                new SimpleStringSchema(),
                                sourceProperties);
                DataStream<String> rulesStrings = env.addSource(kinesisConsumer)
                                .name("RulesStream")
                                .uid("rules-stream");
                return rulesStrings.flatMap(new RuleDeserializer()).name("Rule Deserialization");
        }

        /**
         * Creates a KinesisStreamsSink for writing alert data to a Kinesis stream.
         *
         * @return A KinesisStreamsSink for alert data
         * @throws IOException if an error occurs while reading Kinesis properties
         */
        private KinesisStreamsSink<String> createAlertSink(Properties sinkProperties) throws IOException {
                String ALERTS_TOPIC = KinesisUtils.getKinesisRuntimeProperty("kinesis", "alertsTopicARN");
                KinesisStreamsSink<String> kdsSink = KinesisStreamsSink.<String>builder()
                                .setSerializationSchema(new SimpleStringSchema())
                                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                                .setKinesisClientProperties(sinkProperties)
                                .setStreamArn(ALERTS_TOPIC)
                                .build();
                return kdsSink;
        }

        /**
         * Creates a KinesisStreamsSink for writing rule operation status data to a
         * Kinesis stream.
         *
         * @return A KinesisStreamsSink for rule operation status data
         * @throws IOException if an error occurs while reading Kinesis properties
         */
        private KinesisStreamsSink<String> createRuleOperationsSink(Properties sinkProperties) throws IOException {
                String RULE_OPS_TOPIC = KinesisUtils.getKinesisRuntimeProperty("kinesis", "ruleOperationsTopicARN");
                KinesisStreamsSink<String> kdsSink = KinesisStreamsSink.<String>builder()
                                .setSerializationSchema(new SimpleStringSchema())
                                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                                .setStreamArn(RULE_OPS_TOPIC)
                                .setKinesisClientProperties(sinkProperties)
                                .build();
                return kdsSink;
        }

        private Properties createSourceProperties() throws IOException {
                Properties sourceProperties = new Properties();
                String region = KinesisUtils.getKinesisRuntimeProperty("kinesis", "region");
                String streamPosition = KinesisUtils.getKinesisRuntimeProperty("kinesis", "streamPosition");
                sourceProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
                sourceProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, streamPosition);
                return sourceProperties;
        }

        private Properties createSinkProperties() throws IOException {
                Properties sinkProperties = new Properties();
                String region = KinesisUtils.getKinesisRuntimeProperty("kinesis", "region");
                sinkProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
                return sinkProperties;
        }

}
