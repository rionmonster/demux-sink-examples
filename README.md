# Flink Demultiplexing Sink Examples

A collection of end-to-end samples that demonstrate how to route Apache Flink 2.x streams to multiple destinations at runtime using a custom **Demultiplexing Sink**. Each example pairs production-style sink implementations with integration tests powered by Testcontainers so you can see how dynamic routing behaves against real Kafka, PostgreSQL, Elasticsearch, and filesystem backends.

## Highlights
- Builds on the Flink Sink V2 API with a `DemultiplexingSink` that caches writers per route and restores them from checkpoints for fault tolerance.
- Uses a simple `SinkRouter` contract so you can decide how to derive a route key and create the underlying sink instance for that route.
- Provides runnable examples for FileSink, KafkaSink, JDBC (PostgreSQL), and Elasticsearch, plus a `RoutableMessage` helper for JSON payloads.
- Ships with comprehensive integration tests that spin up the target services via Testcontainers and assert ordering, batching, and dynamic route creation.

## Repository Layout
- `src/main/java/org/rionmonster/flink/examples` – core sink implementation (`DemultiplexingSink`, writer/state helpers, `SinkRouter`) and example jobs under connector-specific packages.
- `src/test/java/org/rionmonster/flink/examples` – integration tests and utility routers/writers for each connector.
- `pom.xml` – Maven build targeting Java 17 and Flink 2.0.0 with connector/test dependencies.

## Prerequisites
- Java 17 (matching the compiler target in `pom.xml`)
- Maven 3.9+
- Docker with at least 6 GB of free memory (required for the Testcontainers-based integration tests)
- Optional: A local Flink 2.x distribution if you want to submit the assembled examples to a running cluster

## Build & Test

Run the full build, including all integration tests:

```bash
mvn clean verify
```

> **Note:** Integration tests start Kafka, PostgreSQL, and Elasticsearch containers. Ensure Docker is running. To skip them while iterating, add `-DskipITs`.

## Running the Sample Jobs

The connectors in `src/main` are self-contained jobs that create a `DemultiplexingSink` and feed a few sample records via `StreamExecutionEnvironment#fromData(...)`. Replace the placeholder connection details with values for your environment, then:

1. Build the shaded application JAR (skip tests if you only need the artifact):
   ```bash
   mvn clean package -DskipTests
   ```
2. Submit one of the examples to your Flink cluster, e.g. the Kafka variant:
   ```bash
   flink run \
     -c org.rionmonster.flink.examples.kafka.KafkaDemultiplexingSinkExample \
     target/demux-sink-examples-1.0-SNAPSHOT.jar
   ```

Each example demonstrates a different routing strategy—by topic for Kafka, by index for Elasticsearch, by table for JDBC, or by directory/file prefix for FileSink.

## Customising the Demultiplexing Sink

Extending the examples typically involves implementing your own `SinkRouter`. Provide deterministic routing keys and create a connector-specific sink when a new route is encountered. For reference, the Kafka example routes messages based on the JSON `topic` field:

```19:46:src/main/java/org/rionmonster/flink/examples/kafka/KafkaDemultiplexingSinkExample.java
streamEnv
    .fromData("a:this", "a:is", "b:a", "c:file", "b:routing", "a:example")
    .sinkTo(
        new DemultiplexingSink<>(
            new SinkRouter<String, String>() {
                @Override
                public String getRoute(String element) {
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
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
                }
            })));
```

Swap in your own route derivation logic, instantiate the appropriate sink, and Flink will manage the lifecycle of each per-route writer—including state snapshots—through `DemultiplexingSink`.

## Useful Maven Targets
- `mvn test` – runs unit tests and lightweight integration tests (skipping the long-running ITs if configured via `-DskipITs`)
- `mvn verify -Dtest=...` – focus on specific suites, e.g. `KafkaSinkDemultiplexingIT`
- `mvn dependency:tree` – inspect dependency clashes when integrating into a larger build

## Troubleshooting
- **Containers fail to start:** Check Docker resource limits. Elasticsearch in particular needs ~2 GB RAM.
- **Port conflicts:** Testcontainers picks random ports; set `TESTCONTAINERS_RYUK_DISABLED=true` or `TESTCONTAINERS_HOST_OVERRIDE` if your environment requires it.
- **ClassNotFoundException for connectors:** Make sure you are packaging connector dependencies with your job or providing them as part of the Flink distribution’s `lib/` directory.

## License

This project is provided under the Apache License 2.0. See the headers inside the source files for details.
