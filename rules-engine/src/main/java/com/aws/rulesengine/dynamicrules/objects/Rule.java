package com.aws.rulesengine.dynamicrules.objects;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@Data
@NoArgsConstructor(force = true)
@AllArgsConstructor
public class Rule {
    @NonNull
    private String id;
    @NonNull
    private String name;
    @NonNull
    private Status status;
    @NonNull
    private String equipmentName;
    @NonNull
    private String ruleExpression;
    @NonNull
    @Singular
    private Map<String, Integer> sensorWindowMap;
}