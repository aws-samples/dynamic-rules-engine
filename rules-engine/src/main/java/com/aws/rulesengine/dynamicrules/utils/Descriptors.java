package com.aws.rulesengine.dynamicrules.utils;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

import com.aws.rulesengine.dynamicrules.objects.Rule;
import com.aws.rulesengine.dynamicrules.objects.RuleOperationStatus;

public class Descriptors {
    public static final MapStateDescriptor<String, Rule> rulesDescriptor = new MapStateDescriptor<>(
                    "rules", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<RuleOperationStatus> ruleOperations = new OutputTag<RuleOperationStatus>(
                    "rule-operations") {
    };
}