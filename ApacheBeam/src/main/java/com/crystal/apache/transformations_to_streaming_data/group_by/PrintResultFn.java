package com.crystal.apache.transformations_to_streaming_data.group_by;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintResultFn<T> extends DoFn<T, T> {
    Logger log = LoggerFactory.getLogger(PrintResultFn.class);

    @ProcessElement
    public void processElement(
            @Element T element, OutputReceiver<T> out, BoundedWindow window) {

        String message = element.toString();

        if (!(window instanceof GlobalWindow)) {
            message = message + "  Window:" + window.toString();
        }

        log.info(message);

        out.output(element);
    }
}
