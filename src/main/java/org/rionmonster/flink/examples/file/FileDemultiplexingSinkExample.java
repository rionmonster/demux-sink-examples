package org.rionmonster.flink.examples.file;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.SinkRouter;

import java.time.Duration;

public class FileDemultiplexingSinkExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        streamEnv
            .fromData("this", "is", "a", "file", "routing", "example")
            .sinkTo(
                new DemultiplexingSink<>(
                    new SinkRouter<String, Character>() {
                        @Override
                        public Character getRoute(String element) {
                            // Simple lexicographical routing
                            return element.charAt(0);
                        }

                        @Override
                        public Sink<String> createSink(Character route, String element) {
                            // Define where and how you would want to write your files here
                            Path routePath = new Path("your-output-location/" + route);

                            // Barebones streaming file sink for your route
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
                )
            );

        // Run the job
        streamEnv.execute("FileSink Demultiplexing Job");
    }
}
