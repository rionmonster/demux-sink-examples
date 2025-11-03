package org.rionmonster.flink.examples.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.rionmonster.flink.examples.DemultiplexingSink;
import org.rionmonster.flink.examples.SinkRouter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchDemultiplexingSinkExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * This example takes a series of colon-separated elements in the format: $index:$message
         * by dynamically adding elements to the corresponding index.
         */
        streamEnv
            .fromData("a:this", "a:is", "b:a", "c:file", "b:routing", "a:example")
            .sinkTo(
                new DemultiplexingSink<>(
                    new SinkRouter<String, String>() {
                        @Override
                        public String getRoute(String element) {
                            // Assumes all records will be prefixed with a index name followed by a colon
                            return element.split(":")[0];
                        }

                        @Override
                        public Sink<String> createSink(String indexName, String element) {
                            return new TestElasticsearchSink(
                                "http://your-elasticsearch:9200",
                                    indexName
                            );
                        }
                    }
                )
            );

        // Run the job
        streamEnv.execute("Elasticsearch Demultiplexing Job");
    }

    private record TestElasticsearchSink(String httpHostAddress, String indexName) implements Sink<String> {

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new SimpleElasticsearchWriter(httpHostAddress, indexName);
        }

        static class SimpleElasticsearchWriter implements SinkWriter<String> {

            private final String indexName;
            private final RestClient restClient;
            private final RestClientTransport transport;
            private final ElasticsearchClient client;

            SimpleElasticsearchWriter(String httpHostAddress, String indexName) {
                this.indexName = indexName;
                HttpHost httpHost = HttpHost.create(httpHostAddress);
                this.restClient = RestClient.builder(httpHost).build();
                this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                this.client = new ElasticsearchClient(transport);
            }

            @Override
            public void write(String element, Context context) {
                try {
                    Map<String, Object> document = new HashMap<>();
                    document.put("message", element);
                    document.put("ingested_at", System.currentTimeMillis());

                    client.index(idx -> idx.index(indexName).document(document));
                } catch (Exception e) {
                    throw new RuntimeException("Error indexing document to " + indexName, e);
                }
            }

            @Override
            public void flush(boolean endOfInput) {
                // Elasticsearch client handles flushing internally
            }

            @Override
            public void close() throws Exception {
                try {
                    transport.close();
                } finally {
                    restClient.close();
                }
            }
        }
    }
}
