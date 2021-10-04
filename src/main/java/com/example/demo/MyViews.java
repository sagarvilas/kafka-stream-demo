package com.example.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MyViews {

    @Autowired
    public void buildProductView(StreamsBuilder sb) {
        final Serde<Product> productSerde = Serdes.serdeFrom(new JsonSerializer<>(), new ProductDeserializer());
        final Serde<Price> valueSerde = Serdes.serdeFrom(new JsonSerializer<>(), new PriceDeserializer());

        final KTable<Integer, Product> products = sb.stream("products", Consumed.with(Serdes.Integer(), productSerde))
                .selectKey((k, v) -> v.getId())
                .toTable(Materialized.<Integer, Product, KeyValueStore<Bytes, byte[]>>as("products-view")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(productSerde));

        final KTable<Integer, Price> prices = sb.stream("prices", Consumed.with(Serdes.Integer(), valueSerde))
                .selectKey((k, v) -> v.getId())
                .toTable(Materialized.<Integer, Price, KeyValueStore<Bytes, byte[]>>as("prices-view")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(valueSerde));

        final KTable<Integer, Order> join = products.join(prices, Product::getId, (l, r) ->
                new Order(l.getId(), l.getItem(), r.getPrice())
        );

        join.toStream().foreach((x, y) -> System.out.println(y));

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Product {
        Integer id;
        String item;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Price {
        Integer id;
        Integer price;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public class Order {
        Integer id;
        String item;
        Integer price;
    }
}
