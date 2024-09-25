package com.aws.rulesengine.dynamicrules.exceptions;

import com.aws.rulesengine.dynamicrules.objects.OperationStatus;
import com.aws.rulesengine.dynamicrules.objects.Rule;

public class InsufficientDataException extends Exception {

    private Rule rule;
    private OperationStatus status = OperationStatus.INSUFFICIENT_DATA;

    public InsufficientDataException(String message) {
        super(message);
    }

    public InsufficientDataException(Rule rule) {
        super(String.format("Insufficient data for rule %s", rule));
        this.rule = rule;
    }

    public Rule getRule() {
        return rule;
    }

    public OperationStatus getStatus() {
        return status;
    }

}

