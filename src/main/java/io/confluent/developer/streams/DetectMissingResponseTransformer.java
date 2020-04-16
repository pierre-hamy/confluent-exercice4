package io.confluent.developer.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Date;

public class DetectMissingResponseTransformer implements org.apache.kafka.streams.kstream.Transformer<String, String, KeyValue<String, String>> {
    private ProcessorContext localContext;
    private KeyValueStore inflightMessageStore;

    @Override
    public void init(ProcessorContext context) {
        localContext = context;
        inflightMessageStore = (KeyValueStore) context.getStateStore("inflightMessageStore");
        // TODO IMPLEMENT ME
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        // TODO
        return null;
    }

    @Override
    public void close() {

    }
}
