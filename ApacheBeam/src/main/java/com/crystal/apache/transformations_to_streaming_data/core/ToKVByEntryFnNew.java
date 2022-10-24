package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.lang.reflect.Field;

public class ToKVByEntryFnNew<K,V> extends DoFn<V, KV<K, V>> {
    private String fieldName;

    public ToKVByEntryFnNew(String fieldName) {
        this.fieldName = fieldName;
    }


    @ProcessElement
    public void transformToKV(ProcessContext c) throws NoSuchFieldException, IllegalAccessException {
        V v = c.element();

        Field field = v.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        K key = (K) field.get(v);


        c.output(KV.of(key, v));
    }
}
