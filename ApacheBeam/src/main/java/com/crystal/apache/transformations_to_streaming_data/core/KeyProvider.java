package com.crystal.apache.transformations_to_streaming_data.core;

import java.io.Serializable;

public interface KeyProvider<K, V> extends Serializable {
    K getKey(V v);
}
