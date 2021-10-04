package com.example.demo;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ProductSerDe implements Serde<MyViews.Product> {
    @Override
    public Serializer<MyViews.Product> serializer() {
        return null;
    }

    @Override
    public Deserializer<MyViews.Product> deserializer() {
        return null;
    }
}
