package com.aws.rulesengine.dynamicrules.objects;

import javax.annotation.Nullable;

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
public class Equipment {
    @NonNull
    private String id;
    @Nullable
    private String type;
    @Nullable
    private String name;

}
