package com.example.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MyViews {

  @Autowired
  public void buildProductView(StreamsBuilder sb) {
    final Serde<Product> productSerde =
        Serdes.serdeFrom(new JsonSerializer<>(), new ProductDeserializer());
    final Serde<Price> priceSerde =
        Serdes.serdeFrom(new JsonSerializer<>(), new PriceDeserializer());
    final Serde<Order> orderSerde =
        Serdes.serdeFrom(new JsonSerializer<>(), new OrderDeserializer());

    final KStream<Integer, Product> products =
        sb.stream("products", Consumed.with(Serdes.Integer(), productSerde))
            .selectKey((k, v) -> v.getId())
            .toTable(
                Materialized.<Integer, Product, KeyValueStore<Bytes, byte[]>>as("products-view")
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(productSerde))
            .toStream();

    final KStream<Integer, Price> prices =
        sb.stream("prices", Consumed.with(Serdes.Integer(), priceSerde))
            .selectKey((k, v) -> v.getId())
            .toTable(
                Materialized.<Integer, Price, KeyValueStore<Bytes, byte[]>>as("prices-view")
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(priceSerde))
            .toStream();

    final KTable<Integer, Order> join =
        products
            .join(
                prices,
                (l, r) -> new Order(l.getId(), l.getItem(), r.getPrice()),
                JoinWindows.of(Duration.ofMinutes(5)),
                StreamJoined.with(
                    Serdes.Integer(), /* key */ productSerde, /* left value */ priceSerde /* right value */))
            .toTable(
                Materialized.<Integer, Order, KeyValueStore<Bytes, byte[]>>as("order-view")
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(orderSerde));

//    join.toStream().foreach((x, y) -> System.out.println(y));
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
  public static class Order {
    Integer id;
    String item;
    Integer price;
  }
}
