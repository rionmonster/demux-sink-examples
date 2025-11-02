package org.rionmonster.flink.examples.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.rionmonster.flink.examples.RoutableMessage;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public record JdbcSink(
    String jdbcUrl,
    String username,
    String password,
    String tableName
) implements Sink<String>, Serializable {

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new SimpleJdbcSinkWriter(jdbcUrl, username, password, tableName);
    }

    private static class SimpleJdbcSinkWriter implements SinkWriter<String>, Serializable {

        private final transient Connection connection;
        private final transient PreparedStatement preparedStatement;
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private final String tableName;
        private int batchCount = 0;

        public SimpleJdbcSinkWriter(String jdbcUrl, String username, String password, String tableName) {
            this.tableName = tableName;

            try {
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection(jdbcUrl, username, password);
                connection.setAutoCommit(false);

                String sql = "INSERT INTO " + tableName + " (payload, ingested_at) VALUES (?, ?)";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("Error connecting to PostgreSQL", e);
            }
        }

        @Override
        public void write(String element, Context context) {
            try {
                RoutableMessage message = MAPPER.readValue(element, RoutableMessage.class);
                preparedStatement.setString(1, message.payload());
                preparedStatement.setLong(2, System.currentTimeMillis());
                preparedStatement.addBatch();

                batchCount++;
                int batchSize = 20;
                if (batchCount >= batchSize) {
                    executeBatch();
                }
            } catch (Exception e) {
                throw new RuntimeException("Error writing to PostgreSQL table " + tableName, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            executeBatch();
        }

        private void executeBatch() {
            if (batchCount > 0) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();
                    batchCount = 0;
                } catch (Exception e) {
                    try {
                        connection.rollback();
                    } catch (Exception ignore) {
                        // ignore rollback errors
                    }
                    throw new RuntimeException("Error executing batch for table " + tableName, e);
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                executeBatch();
            } finally {
                try {
                    if (preparedStatement != null) preparedStatement.close();
                } finally {
                    if (connection != null) connection.close();
                }
            }
        }
    }
}
