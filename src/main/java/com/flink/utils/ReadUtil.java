package com.flink.utils;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Properties;

public class ReadUtil {

    public static KafkaSource<JsonNode> getSource(Properties properties, String inputTopic) {
        KafkaSource<JsonNode> source = KafkaSource.<JsonNode>builder()
                .setProperties(properties)
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(JsonNode.class))
                .build();

        System.out.println("Source  : " + source + " FileName : " + ReadUtil.class);

        return source;
    }
}
