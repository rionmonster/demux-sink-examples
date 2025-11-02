package org.rionmonster.flink.examples.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.RoutableMessage;
import org.rionmonster.flink.examples.SinkRouter;
import org.rionmonster.flink.examples.SinkTestUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class JdbcSinkDemultiplexingIT {

    @Container
    @SuppressWarnings("resource")
    public static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("test_db")
            .withUsername("test_user")
            .withPassword("test_password");

    @BeforeEach
    void setup() throws Exception {
        // Create/clear test tables
        try (Connection connection = POSTGRES.createConnection("")) {
            try (var statement = connection.createStatement()) {
                statement.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        payload TEXT NOT NULL,
                        ingested_at BIGINT NOT NULL
                    )
                """);
                statement.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        payload TEXT NOT NULL,
                        ingested_at BIGINT NOT NULL
                    )
                """);
                statement.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id SERIAL PRIMARY KEY,
                        payload TEXT NOT NULL,
                        ingested_at BIGINT NOT NULL
                    )
                """);
                statement.execute("TRUNCATE TABLE orders, users, events");
            }
        }
    }

    @Test
    @DisplayName("should route messages to different PostgreSQL tables based on topic")
    void shouldRouteMessagesToDifferentPostgreSQLTablesBasedOnTopic() throws Exception {
        // Arrange
        List<String> messages = List.of(
            "{\"topic\":\"orders\",\"payload\":\"Order #1001 - Customer A\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #1002 - Customer B\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #1003 - Customer C\"}",
            "{\"topic\":\"users\",\"payload\":\"alice@example.com\"}",
            "{\"topic\":\"users\",\"payload\":\"bob@example.com\"}",
            "{\"topic\":\"events\",\"payload\":\"System startup event\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(getDynamicTopicRouter()));

        SinkTestUtils.executeAndWait(streamEnv, 1000);

        // Assert
        assertTablesContainsExpectedPayloads(Map.of(
            "orders", Set.of(
                "Order #1001 - Customer A",
                "Order #1002 - Customer B",
                "Order #1003 - Customer C"
            ),
            "users", Set.of(
                "alice@example.com",
                "bob@example.com"
            ),
            "events", Set.of(
                "System startup event"
            )
        ));
    }

    @Test
    @DisplayName("should handle batching and transactions correctly")
    void shouldHandleBatchingAndTransactionsCorrectly() throws Exception {
        // Arrange
        List<String> messages = java.util.stream.IntStream.rangeClosed(1, 50)
            .mapToObj(i -> "{\"topic\":\"orders\",\"payload\":\"Order #" + (1000 + i) + "\"}")
            .toList();

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(getSpecificTableRouter("orders")));
        SinkTestUtils.executeAndWait(streamEnv, 1000);

        // Assert
        assertTableContainsCorrectCount("orders", 50);
    }

    @Test
    @DisplayName("should handle dynamic table routing")
    void shouldHandleDynamicTableRouting() throws Exception {
        // Arrange
        List<String> messages = List.of(
            "{\"topic\":\"orders\",\"payload\":\"Order A\"}",
            "{\"topic\":\"users\",\"payload\":\"User A\"}",
            "{\"topic\":\"events\",\"payload\":\"Event A\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order B\"}",
            "{\"topic\":\"users\",\"payload\":\"User B\"}",
            "{\"topic\":\"events\",\"payload\":\"Event B\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order C\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(getDynamicTopicRouter()));
        SinkTestUtils.executeAndWait(streamEnv, 1000);

        // Assert
        assertTablesContainsExpectedPayloads(Map.of(
            "orders", Set.of(
                "Order A",
                "Order B",
                "Order C"
            ),
            "users", Set.of(
                "User A",
                "User B"
            ),
            "events", Set.of(
                "Event A",
                "Event B"
            )
        ));
    }

    private void assertTablesContainsExpectedPayloads(Map<String, Set<String>> expectedContents) throws SQLException {
        try (Connection connection = POSTGRES.createConnection("")) {
            for (Map.Entry<String, Set<String>> expectation : expectedContents.entrySet()){
                String targetTable = expectation.getKey();
                try (Statement st = connection.createStatement()) {
                    try (ResultSet rs = st.executeQuery("SELECT payload FROM " + targetTable)) {
                        var idsSeen = new HashSet<String>();
                        while (rs.next()) {
                            idsSeen.add(rs.getString("payload"));
                        }

                        Set<String> expectedIds = expectation.getValue();
                        Assertions.assertThat(idsSeen).hasSize(expectedIds.size());
                        Assertions.assertThat(idsSeen).containsAll(expectedIds);
                    }
                }
            }
        }
    }

    private void assertTableContainsCorrectCount(String targetTable, Integer expectedCount) throws SQLException {
        try (Connection connection = POSTGRES.createConnection("")) {
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) as count FROM " + targetTable)) {
                    rs.next();
                    Integer actualCount = rs.getInt("count");

                    assertThat(actualCount).isEqualTo(expectedCount);
                }
            }
        }
    }

    private static SinkRouter<String, String> getDynamicTopicRouter() {
        final String jdbcUrl = POSTGRES.getJdbcUrl();
        final String username = POSTGRES.getUsername();
        final String password = POSTGRES.getPassword();

        return new SinkRouter<>() {
            private static final ObjectMapper MAPPER = new ObjectMapper();

            @Override
            public String getRoute(String element) {
                try {
                    return MAPPER.readValue(element, RoutableMessage.class).topic();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Sink<String> createSink(String route, String element) {
                return new JdbcSink(jdbcUrl, username, password, route);
            }
        };
    }

    private static SinkRouter<String, String> getSpecificTableRouter(String targetTable) {
        final String jdbcUrl = POSTGRES.getJdbcUrl();
        final String username = POSTGRES.getUsername();
        final String password = POSTGRES.getPassword();

        return new SinkRouter<>() {
            @Override
            public String getRoute(String element) {
                return targetTable;
            }

            @Override
            public Sink<String> createSink(String route, String element) {
                return new JdbcSink(jdbcUrl, username, password, route);
            }
        };
    }
}


