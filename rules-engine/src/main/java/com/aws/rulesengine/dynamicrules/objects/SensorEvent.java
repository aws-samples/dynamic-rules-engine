package com.aws.rulesengine.dynamicrules.objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@Data
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Builder
@JsonIgnoreProperties({ "asset_uuid", "measure_name", "utc_date"})
public class SensorEvent implements TimestampAssignable<Long> {
    @NonNull
    private Equipment equipment;
    @NonNull
    @JsonAlias({ "point_uuid", "pointUuid", "id" })
    private String id;
    @NonNull
    @JsonAlias({ "measure_value", "measureValue" })
    private Double measureValue;
    @NonNull
    @JsonAlias({ "utc_date_ms", "eventTimestamp" })
    private Long eventTimestamp;
    private Long ingestionTimestamp;

    @Override
    public void assignIngestionTimestamp(Long timestamp) {
        this.ingestionTimestamp = timestamp;
    }
}

