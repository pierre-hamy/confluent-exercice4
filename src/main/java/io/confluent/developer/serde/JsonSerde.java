package io.confluent.developer.serde;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Simple Serde parsing and serializing JSON based on GSON
 * TODO: In the POC, a generic GSON has been used. An object
 * mapper would easier to maintain in a long project
 * e.g.: https://github.com/apache/kafka/blob/2.3/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java#L83
 */
public class JsonSerde<T> implements Serde, Serializer<T>, Deserializer {

    private final Gson gson;
    private Class<T> type;

    public static <T> JsonSerde<T> create(Class<T> type) {
        return new JsonSerde(type);
    }

    public static JsonSerde<JsonObject> create() {
        return new JsonSerde(JsonObject.class);
    }

    public JsonSerde(Class<T> type) {
        this.type = type;
        gson = new Gson();
    }

    public void configure(Map configs, boolean isKey) {
    }

    public void close() {

    }

    public byte[] serialize(String topic, T data) {
        try {
            return gson.toJson(data, type).getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            // TODO: Should we do a better handling of different charset?
            return gson.fromJson(new String(data), type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        try {
            // TODO: Should we do a better handling of different charset?
            return gson.fromJson(new String(data), type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }


    public Serializer<T> serializer() {
        return this;
    }

    public Deserializer deserializer() {
        return this;
    }
}
