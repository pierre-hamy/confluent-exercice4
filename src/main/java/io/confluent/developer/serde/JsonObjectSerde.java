package io.confluent.developer.serde;

import com.google.gson.JsonObject;

public class JsonObjectSerde extends JsonSerde<JsonObject> {
    public JsonObjectSerde() {
        super(JsonObject.class);
    }
}
