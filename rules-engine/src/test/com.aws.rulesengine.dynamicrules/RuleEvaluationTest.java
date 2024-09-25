package com.aws.rulesengine.dynamicrules;

import com.carrier.rulesengine.dynamicrules.objects.SensorEvent;
import com.carrier.rulesengine.dynamicrules.objects.SensorMapState;
import org.apache.commons.jexl3.*;
import org.apache.flink.api.common.state.MapState;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RuleEvaluationTest {
    private static final JexlEngine jexl = new JexlBuilder()
            .permissions(JexlPermissions.RESTRICTED.compose("com.aws.rulesengine.dynamicrules.*")).cache(512)
            .strict(true).silent(false)
            .create();

    private transient SensorMapState sensorMapState;
    private JsonMapper jsonMapper = new JsonMapper(SensorEvent.class);
    @Before
    public void setup() throws IOException {
        sensorMapState = new SensorMapState();
        SensorEvent event1 = new SensorEvent();
        event1.setId("SENSOR_9d5ef9bf_a7bf43368f60d1a_9d9d12c2a");
        event1.setMeasureValue(7D);
        event1.setEventTimestamp(System.currentTimeMillis()-5000);
        sensorMapState.addSensorEvent(event1);
        SensorEvent event2 = new SensorEvent();
        event2.setId("SENSOR_9d5ef9bf_a7bf43368f60d1a_9d9d12c2a");
        event2.setMeasureValue(7D);
        event2.setEventTimestamp(System.currentTimeMillis()-4000);
        sensorMapState.addSensorEvent(event2);
    }

    @Test
    public void testRuleEvaluation() throws Exception {
        String SENSOR_9d5ef9bfa7bf43368f60d1a9d9d12c2a = "";
        String rule = "A.getSecondsSinceValueLessThan(3.0) >= 4.0 and A.getSecondsSinceValueChange() >= 1.0 and A.getSecondsSinceValueBetween(1.0,4.0) >= 5.0 and A.getAverageSensorValue() >= 1.0";
        JexlExpression expression = jexl.createExpression(rule);
        JexlContext context = new MapContext();
        context.set("A", sensorMapState);
        boolean result = (boolean) expression.evaluate(context);
        assertEquals(true, result);
    }

    @Test
    public void testRuleEvaluationWithLongNames() throws Exception {
        String rule = "SENSOR_9d5ef9bf_a7bf43368f60d1a_9d9d12c2a.hasChanged(30)";
        JexlExpression expression = jexl.createExpression(rule);
        JexlContext context = new MapContext();
        context.set("SENSOR_9d5ef9bf_a7bf43368f60d1a_9d9d12c2a", sensorMapState);
        boolean result = (boolean) expression.evaluate(context);
        assertEquals(true, result);
    }

}
