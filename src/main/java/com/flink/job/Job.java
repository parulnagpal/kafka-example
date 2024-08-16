package com.flink.job;

// to run this program use below command line arguments
// --input.topic input-stream --bootstrap.servers 142.171.227.20:29092 --zookeeper.connect 142.171.227.20:22181 --group.id demo --output.topic output-stream

import com.flink.utils.ReadUtil;
import com.flink.utils.WriteUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
    public static void main(String[] args) throws Exception {

        System.out.println("Reading command line arguments....");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        KafkaSource<JsonNode> source = ReadUtil.getSource(parameterTool.getProperties(),parameterTool.get("input.topic"));
        DataStreamSource<JsonNode> messageStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");
        KafkaRecordSerializationSchema<JsonNode> messageSerializer = WriteUtil.createMessageSerializer(parameterTool.get("output.topic"));
        KafkaSink<JsonNode> messageKafkaSink = WriteUtil.createKafkaSink(parameterTool, messageSerializer);
        messageStream.sinkTo(messageKafkaSink).name("sink");

        env.execute();

    }

}
