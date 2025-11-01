package org.rionmonster.flink.examples.file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import org.rionmonster.flink.examples.DemultiplexingSink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test for DemultiplexingSink with FileSink backend.
 *
 * This test:
 * 1. Creates test messages with different topics
 * 2. Routes messages to different file system directories based on topic
 * 3. Verifies that files are created in the correct directories
 * 4. Validates that message content is correctly written
 */
public class FileSinkDemultiplexingIT {

    @TempDir
    File tempDir;

    @Test
    @DisplayName("should route messages to different directories based on topic")
    void shouldRouteMessagesToDifferentDirectoriesBasedOnTopic() throws Exception {
        // Arrange
        List<String> messages = List.of(
            "{\"topic\":\"orders\",\"payload\":\"Order #1001\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #1002\"}",
            "{\"topic\":\"orders\",\"payload\":\"Order #1003\"}",
            "{\"topic\":\"users\",\"payload\":\"alice@example.com\"}",
            "{\"topic\":\"users\",\"payload\":\"bob@example.com\"}",
            "{\"topic\":\"events\",\"payload\":\"System startup\"}"
        );

        // Act
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();
        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(
                new FileSinkRouter(
                    tempDir.getAbsolutePath())
                )
            );
        SinkTestUtils.executeAndWait(streamEnv, 1000);

        // Assert
        assertDirectoryExistsWithExpectedContent(Map.of(
            "orders", Set.of(
                "Order #1001",
                "Order #1002",
                "Order #1003"
            ),
            "users", Set.of(
                "alice@example.com",
                "bob@example.com"
            ),
            "events", Set.of(
                "System startup"
            )
        ));
    }

    @Test
    @DisplayName("should handle dynamic route creation")
    void shouldHandleDynamicRouteCreation() throws Exception {
        // Arrange
        List<String> messages = List.of(
            "{\"topic\":\"topic-a\",\"payload\":\"Data for A\"}",
            "{\"topic\":\"topic-b\",\"payload\":\"Data for B\"}",
            "{\"topic\":\"topic-c\",\"payload\":\"Data for C\"}",
            "{\"topic\":\"topic-a\",\"payload\":\"More data for A\"}",
            "{\"topic\":\"topic-d\",\"payload\":\"Data for D\"}"
        );

        // When: Process messages
        StreamExecutionEnvironment streamEnv = SinkTestUtils.createTestStreamEnvironment();

        streamEnv
            .fromData(messages)
            .sinkTo(new DemultiplexingSink<>(
                new FileSinkRouter(
                    tempDir.getAbsolutePath())
            ));
        SinkTestUtils.executeAndWait(streamEnv, 1000);

        assertDirectoryExistsWithExpectedContent(Map.of(
            "topic-a", Set.of(
                "Data for A",
                "More data for A"
            ),
            "topic-b", Set.of(
                "Data for B"
            ),
            "topic-c", Set.of(
                "Data for C"
            ),
            "topic-d", Set.of(
                "Data for D"
            )
        ));
    }

    private void assertDirectoryExistsWithExpectedContent(
        Map<String, Set<String>> expectedContentByDirectory
    ) throws IOException {
        // Verify all expectations exist
        for (Map.Entry<String, Set<String>> expectedContent : expectedContentByDirectory.entrySet()){
            // Verify directory exists
            String expectedDirectory = expectedContent.getKey();
            File targetDirectory = new File(tempDir, expectedDirectory);
            assertThat(targetDirectory).exists().isDirectory();

            // Verify directory content
            Set<String> expectedValues = expectedContent.getValue();
            var files = listAllFiles(targetDirectory);
            assertThat(files).isNotEmpty();
            var fileContent = readAllLines(files);

            for (String expectedValue : expectedValues){
                assertThat(fileContent).anyMatch(content -> content.contains(expectedValue));
            }
        }
    }

    // Helper Functions

    private static List<java.nio.file.Path> listAllFiles(File dir) throws IOException {
        try (Stream<Path> s = Files.walk(dir.toPath())) {
            return s.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }

    private static List<String> readAllLines(List<java.nio.file.Path> files) throws IOException {
        try (Stream<String> lines =
             files.stream().flatMap(p -> {
                 try {
                     return Files.readAllLines(p).stream();
                 } catch (IOException e) {
                     throw new RuntimeException(e);
                 }
             })) {
            return lines.collect(Collectors.toList());
        }
    }
}
