package org.rionmonster.flink.examples.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.SinkRouter;

public class KafkaDemultiplexingSinkExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        streamEnv
            .fromData("a:this", "a:is", "b:a", "c:file", "b:routing", "a:example")
            .sinkTo(
                new DemultiplexingSink<>(
                    new SinkRouter<String, String>() {
                        @Override
                        public String getRoute(String element) {
                            // Assumes all records will be prefixed with a topic name followed by a colon
                            return element.split(":")[0];
                        }

                        @Override
                        public Sink<String> createSink(String topicName, String element) {
                            return KafkaSink.<String>builder()
                                .setBootstrapServers("your-bootstrap-servers")
                                .setRecordSerializer(
                                    KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic(topicName)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                                )
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .build();
                        }
                    }
                )
            );

        // Run the job
        streamEnv.execute("Kafka Demultiplexing Job");
    }
}
