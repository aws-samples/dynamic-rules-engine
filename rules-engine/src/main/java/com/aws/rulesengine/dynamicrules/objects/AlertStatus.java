package com.aws.rulesengine.dynamicrules.objects;

public enum AlertStatus {
    STOP(0),
    START(1);

    private final int value;

    AlertStatus(int i) {
        this.value = i;
    }
};
