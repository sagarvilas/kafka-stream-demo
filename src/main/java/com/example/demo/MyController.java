package com.example.demo;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MyController {

    private final StreamsBuilderFactoryBean sbfb;

    @GetMapping("/id/{id}")
    public String getValue(String id) {
        final KafkaStreams kafkaStreams = sbfb.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, String> store =
                kafkaStreams.store(StoreQueryParameters.fromNameAndType("order-view", QueryableStoreTypes.keyValueStore()));
        String result = store.get(id);
        return result;
    }
}
