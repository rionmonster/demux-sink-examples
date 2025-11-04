package org.rionmonster.flink.examples.elasticsearch;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.RoutableMessage;
import org.rionmonster.flink.examples.SinkRouter;
import org.rionmonster.flink.examples.SinkTestUtils;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Integration test for DemultiplexingSink with Elasticsearch backend.
 *
 * This test:
 * 1. Starts an Elasticsearch container via TestContainers
 * 2. Sends test messages through DemultiplexingSink
 * 3. Routes messages to different Elasticsearch indices based on topic field
 * 4. Verifies that messages are correctly indexed in their respective indices
 * 5. Validates document counts
 */
@Testcontainers
public class ElasticsearchSinkDemultiplexingIT {

    @Container
    public static final ElasticsearchContainer elasticsearchContainer =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.11.0")
                    .withEnv("xpack.security.enabled", "false")
                    .withEnv("discovery.type", "single-node");

    private ElasticsearchClient elasticsearchClient;
    private RestClient restClient;

    @BeforeEach
    void setup() {
        HttpHost httpHost = HttpHost.create(elasticsearchContainer.getHttpHostAddress());
        restClient = RestClient.builder(httpHost).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        elasticsearchClient = new ElasticsearchClient(transport);
    }

    @AfterEach
    void teardown() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
    }

    @Test
    void shouldRouteMessagesToDifferentElasticsearchIndicesBasedOnTopic() throws Exception {
        // Arrange
        List<String> messages = Arrays.asList(
            "{\"topic\":\"app1\",\"payload\":\"Application 1 log entry\"}",
            "{\"topic\":\"app1\",\"payload\":\"Another app 1 log\"}",
            "{\"topic\":\"app1\",\"payload\":\"Third app 1 log\"}",
            "{\"topic\":\"app2\",\"payload\":\"Application 2 log entry\"}",
            "{\"topic\":\"app2\",\"payload\":\"Another app 2 log\"}",
            "{\"topic\":\"metrics\",\"payload\":\"System metrics data\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(getTopicRouter()));
        SinkTestUtils.executeAndWait(streamEnv, 2000);

        // Assert
        assertIndicesWithExpectedCounts(Map.of(
            "app1", 3L,
            "app2", 2L,
            "metrics", 1L
        ));
    }

    @Test
    void shouldHandleDynamicIndexCreationForNewRoutes() throws Exception {
        // Arrange
        List<String> messages = Arrays.asList(
            "{\"topic\":\"index-a\",\"payload\":\"Data for index A\"}",
            "{\"topic\":\"index-b\",\"payload\":\"Data for index B\"}",
            "{\"topic\":\"index-c\",\"payload\":\"Data for index C\"}",
            "{\"topic\":\"index-a\",\"payload\":\"More data for index A\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
                .fromData(messages)
                .sinkTo(new DemultiplexingSink<>(getTopicRouter()));
        SinkTestUtils.executeAndWait(streamEnv, 2000);

        // Then
        assertIndicesWithExpectedCounts(Map.of(
        "index-a", 2L,
        "index-b", 1L,
        "index-c", 1L
        ));
    }

    private void assertIndicesWithExpectedCounts(Map<String, Long> expectedCounts) throws IOException {
        // Refresh all the queryable indices
        elasticsearchClient.indices().refresh(r -> r.index(expectedCounts.keySet().stream().toList()));
        // Verify each index contains the expected number of values
        for (Map.Entry<String, Long> expectation : expectedCounts.entrySet()){
            CountResponse indexCount = elasticsearchClient.count(c -> c.index(expectation.getKey()));
            assertThat(indexCount.count()).isEqualTo(expectation.getValue());
        }
    }

    private static SinkRouter<String, String> getTopicRouter() {
        final String httpHost = elasticsearchContainer.getHttpHostAddress();
        final ObjectMapper objectMapper = new ObjectMapper();

        return new SinkRouter<>() {

            @Override
            public String getRoute(String element) {
                try {
                    return objectMapper.readValue(element, RoutableMessage.class).topic();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Sink<String> createSink(String route, String element) {
                return new TestElasticsearchSink(httpHost, route);
            }
        };
    }
}
