package org.rionmonster.flink.examples.jdbc;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.SinkRouter;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JdbcDemultiplexingSinkExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        streamEnv
            .fromData("a:this", "a:is", "b:a", "c:file", "b:routing", "a:example")
            .sinkTo(
                new DemultiplexingSink<>(
                    new SinkRouter<String, String>() {
                        @Override
                        public String getRoute(String element) {
                            // Assumes all records will be prefixed with a table name followed
                            // by a colon
                            return element.split(":")[0];
                        }

                        @Override
                        public Sink<String> createSink(String route, String element) {
                            return new SampleJdbcSink(
                                "your-jdbc-url",
                                "your-username",
                                "your-password",
                                route
                            );
                        }
                    }
                )
            );

        // Run the job
        streamEnv.execute("JdbcSink Demultiplexing Job");
    }

    private record SampleJdbcSink(
        String jdbcUrl,
        String username,
        String password,
        String tableName
    ) implements Sink<String>, Serializable {
        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return new SimpleJdbcSinkWriter(jdbcUrl, username, password, tableName);
        }

        private static class SimpleJdbcSinkWriter implements SinkWriter<String>, Serializable {

            private final transient Connection connection;
            private final transient PreparedStatement preparedStatement;
            private final String tableName;
            private int batchCount = 0;

            public SimpleJdbcSinkWriter(String jdbcUrl, String username, String password, String tableName) {
                this.tableName = tableName;

                try {
                    Class.forName("org.postgresql.Driver");
                    connection = DriverManager.getConnection(jdbcUrl, username, password);
                    connection.setAutoCommit(false);

                    String sql = "INSERT INTO " + tableName + " (name) VALUES (?)";
                    preparedStatement = connection.prepareStatement(sql);
                } catch (Exception e) {
                    throw new RuntimeException("Error connecting to PostgreSQL", e);
                }
            }

            @Override
            public void write(String element, Context context) {
                try {
                    preparedStatement.setString(1, element);
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
                        if (preparedStatement != null) {
                            preparedStatement.close();
                        }
                    } finally {
                        if (connection != null) {
                            connection.close();
                        }
                    }
                }
            }
        }
    }
}
