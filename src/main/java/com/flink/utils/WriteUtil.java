package com.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class WriteUtil {

    public static KafkaRecordSerializationSchema<JsonNode> createMessageSerializer(String topic) {
        KafkaRecordSerializationSchema<JsonNode> messageSerializer = KafkaRecordSerializationSchema.<JsonNode>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build();
        System.out.println("Messageserializer  : " + messageSerializer + " FileName : " + WriteUtil.class);

        return messageSerializer;
    }

    public static KafkaSink<JsonNode> createKafkaSink(ParameterTool parameterTool, KafkaRecordSerializationSchema<JsonNode> messageSerializer) {

        KafkaSink<JsonNode> messageKafkaSink = KafkaSink.<JsonNode>builder()
                .setKafkaProducerConfig(parameterTool.getProperties())
                .setRecordSerializer(messageSerializer)
                .build();

        System.out.println("MessageKafkaSink  : " + messageKafkaSink + " FileName : " + WriteUtil.class);

        return messageKafkaSink;
    }
}
