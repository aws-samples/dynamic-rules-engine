package com.aws.rulesengine.dynamicrules.objects;

public enum OperationStatus {
    INSUFFICIENT_DATA (0),
    SUCCESS (1),
    FAILURE (2);

    private final int value;

    OperationStatus(int value) {
        this.value = value;
    }
}
