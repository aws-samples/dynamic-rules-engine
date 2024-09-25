package com.aws.rulesengine.dynamicrules.exceptions;

public class RuleNotFoundException extends Exception{

    public RuleNotFoundException(String message) {
        super(message);
    }

    @Override
    public String toString() {
        return "Rule not found";
    }

}
