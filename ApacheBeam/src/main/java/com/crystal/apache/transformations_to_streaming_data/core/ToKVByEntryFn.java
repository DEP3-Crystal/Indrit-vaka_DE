package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.lang.reflect.Field;

public class ToKVByEntryFn<T> extends DoFn<T, KV<String, T>> implements Serializable {
    private String fieldName;

    public ToKVByEntryFn(String fieldName) {
        this.fieldName = fieldName;
    }


    @ProcessElement
    public void transformToKV(ProcessContext c) throws NoSuchFieldException, IllegalAccessException {
        T t = c.element();

        Field field = t.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        String key = field.get(t).toString();
        c.output(KV.of(key, t));
    }
}
