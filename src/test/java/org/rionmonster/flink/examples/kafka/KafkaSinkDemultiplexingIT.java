package org.rionmonster.flink.examples.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.RoutableMessage;
import org.rionmonster.flink.examples.SinkRouter;
import org.rionmonster.flink.examples.SinkTestUtils;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Testcontainers
public class KafkaSinkDemultiplexingIT {

    @Container
    public static final KafkaContainer KAFKA =
        new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withReuse(false);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final List<KafkaConsumer<String, String>> consumers = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setup() {
        consumers.clear();
    }

    @AfterEach
    void cleanup() {
        for (KafkaConsumer<String, String> c : consumers) {
            try {
                c.close();
            } catch (Exception ignored) {}
        }
        consumers.clear();
    }

    @Test
    void verifyMessagesAreRoutedToCorrectKafkaTopics() throws Exception {
        // Arrange
        List<String> messages = Arrays.asList(
            "{\"topic\":\"orders\",\"payload\":\"Order #1\"}",
            "{\"topic\":\"users\",\"payload\":\"user@example.com\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #2\"}",
            "{\"topic\":\"events\",\"payload\":\"System event\"}",
            "{\"topic\":\"users\",\"payload\":\"another_user@example.com\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #3\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(getDynamicKafkaTopicRouter()));
        SinkTestUtils.executeAndWait(streamEnv, 1000);

        // Assert
        assertTopicContainsExpectedContent(Map.of(
            "orders", Set.of(
                "Order #1",
                "Order #2",
                "Order #3"
            ),
            "users", Set.of(
                "user@example.com",
                "another_user@example.com"
            ),
            "events", Set.of(
                "System event"
            )
        ));
    }

    private void assertTopicContainsExpectedContent(Map<String, Set<String>> expectedContent) {
        // Create necessary consumers with supported lookups
        String bootstrapServers = KAFKA.getBootstrapServers();
        Map<String, KafkaConsumer<String, String>> consumerMap = Map.of(
            "orders", createConsumer(
                bootstrapServers,
                "orders-consumer",
                Collections.singletonList("orders")
            ),
            "users", createConsumer(
                bootstrapServers,
                "users-consumer",
                Collections.singletonList("users")
            ),
            "events", createConsumer(
                bootstrapServers,
                "events-consumer",
                Collections.singletonList("events")
            )
        );
        consumers.addAll(consumerMap.values());

        // Verify contents for each
        for (Map.Entry<String, Set<String>> expectation : expectedContent.entrySet()) {
            Set<String> expectedMessages = expectation.getValue();
            List<String> messages = pollForNMessages(consumerMap.get(expectation.getKey()), expectedMessages.size());

            // Sanitize messages (since we just want to verify the payload in this case)
            List<String> payloads = messages.stream().map(element ->
                {
                    try {
                        return MAPPER.readValue(element, RoutableMessage.class).payload();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
            ).toList();

            // Verify contents
            assertThat(payloads).containsExactlyInAnyOrderElementsOf(expectedMessages);
        }
    }

    private static SinkRouter<String, String> getDynamicKafkaTopicRouter() {
        final String bootstrapServers = KAFKA.getBootstrapServers();

        return new SinkRouter<>() {


            @Override
            public String getRoute(String element) {
                try {
                    RoutableMessage message = MAPPER.readValue(element, RoutableMessage.class);
                    return message.topic();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse JSON", e);
                }
            }

            @Override
            public Sink<String> createSink(String route, String element) {
                return KafkaSink.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                            .setTopic(route)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            }
        };
    }


    private KafkaConsumer<String, String> createConsumer(
        String bootstrapServers,
        String groupId,
        List<String> topics
    ) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
    }

    private List<String> pollForNMessages(KafkaConsumer<String, String> consumer, int expectedCount) {
        List<String> messages = new ArrayList<>();
        Duration timeout = Duration.ofSeconds(30);
        long start = System.currentTimeMillis();

        while (messages.size() < expectedCount && System.currentTimeMillis() - start < timeout.toMillis()) {
            var records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(rec -> messages.add(rec.value()));
        }

        return messages;
    }
}
