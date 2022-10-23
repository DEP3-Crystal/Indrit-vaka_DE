package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.transforms.DoFn;

public class RemoveHeadersFn extends DoFn<String, String> {
    private String header;

    public RemoveHeadersFn(String header) {
        this.header = header;
    }

    @ProcessElement
    public void filterHeader(@Element String el, OutputReceiver<String> o) {
        if (el.contains(header) || el.isEmpty()) return;
        o.output(el);
    }
}
