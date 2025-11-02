package org.rionmonster.flink.examples.file;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.rionmonster.flink.examples.RoutableMessage;
import org.rionmonster.flink.examples.SinkRouter;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * Serializable router for file sink tests.
 */
record FileSinkRouter(String basePath) implements SinkRouter<String, String>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String getRoute(String element) {
        try {
            RoutableMessage message = MAPPER.readValue(element, RoutableMessage.class);
            return message.topic();
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse message JSON", e);
        }
    }

    @Override
    public Sink<String> createSink(String route, String element) {
        Path routePath = new Path(basePath + "/" + route);

        return FileSink
            .forRowFormat(routePath, new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(5))
                    .withMaxPartSize(MemorySize.ofMebiBytes(128))
                    .build()
            )
            .build();
    }
}