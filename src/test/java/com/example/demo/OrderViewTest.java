package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

public class OrderViewTest {

    @Test
    void shouldCreateMaterializedView() {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final MyViews orderView = new MyViews();
        orderView.buildProductView(streamsBuilder);
      //  orderView.buildPriceView(streamsBuilder);

        Properties config = new Properties();
        config.putAll(Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092",
                StreamsConfig.EXACTLY_ONCE_BETA, true));

        final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), config);

        final ProductDeserializer deserializer = new ProductDeserializer();
        final TestInputTopic<Integer, MyViews.Product> products =
                topologyTestDriver.createInputTopic("products", Serdes.Integer().serializer(), Serdes.serdeFrom(new JsonSerializer<>(), deserializer).serializer());
        final TestInputTopic<Integer, MyViews.Price> prices =
                topologyTestDriver.createInputTopic("prices", Serdes.Integer().serializer(), Serdes.serdeFrom(new JsonSerializer<>(), new PriceDeserializer()).serializer());

        products.pipeInput(new MyViews.Product(1, "Apple"));
        prices.pipeInput(new MyViews.Price(2, 100));
        products.pipeInput(new MyViews.Product(2, "Samsung"));
        prices.pipeInput(new MyViews.Price(3, 50));
        prices.pipeInput(new MyViews.Price(1, 200));
        prices.pipeInput(new MyViews.Price(4, 25));
        products.pipeInput(new MyViews.Product(3, "Nokia"));
        products.pipeInput(new MyViews.Product(4, "HTC"));

        final KeyValueStore<Integer, MyViews.Product> orderStore = topologyTestDriver.getKeyValueStore("products-view");
        final KeyValueStore<Integer, MyViews.Price> priceStore = topologyTestDriver.getKeyValueStore("prices-view");

        orderStore.get(1);
        orderStore.get(2);

        priceStore.get(1);
    }
}
