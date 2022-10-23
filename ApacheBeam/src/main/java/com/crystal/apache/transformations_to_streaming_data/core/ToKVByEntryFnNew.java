package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.lang.reflect.Field;

public class ToKVByEntryFnNew<T,K> extends DoFn<T, KV<K, T>> implements Serializable {
    private String fieldName;

    public ToKVByEntryFnNew(String fieldName) {
        this.fieldName = fieldName;
    }


    @ProcessElement
    public void transformToKV(ProcessContext c) throws NoSuchFieldException, IllegalAccessException {
        T t = c.element();

        Field field = t.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        K key = (K) field.get(t);


        c.output(KV.of(key, t));
    }
}
