package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class OrderDeserializer implements Deserializer<MyViews.Order> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // nothing to do
    }

    @Override
    public MyViews.Order deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        MyViews.Order data;
        try {
            data = objectMapper.readValue(bytes, MyViews.Order.class );
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {
        // nothing to do
    }
}