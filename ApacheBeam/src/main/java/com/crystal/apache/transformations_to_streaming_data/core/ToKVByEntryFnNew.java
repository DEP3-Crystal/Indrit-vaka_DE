package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ToKVByEntryFnNew<K,V> extends DoFn<V, KV<K, V>>  {
//    private String fieldName;
    private final KeyProvider<K,V> keyProvider;
    private final Coder<KV<K,V>> coder;

    public ToKVByEntryFnNew(KeyProvider<K,V> keyProvider, Coder<KV<K, V>> coder) {
        this.keyProvider = keyProvider;
        this.coder = coder;
    }

    @ProcessElement
    public void transformToKV(ProcessContext c) {
        V value = c.element();

        c.output(KV.of(keyProvider.getKey(value), value));
    }

    @Override
    public TypeDescriptor<KV<K, V>> getOutputTypeDescriptor() {
        return coder.getEncodedTypeDescriptor();
    }
}
