package org.rionmonster.flink.examples.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public record TestElasticsearchSink(String httpHostAddress, String indexName) implements Sink<String> {

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new SimpleElasticsearchWriter(httpHostAddress, indexName);
    }

    static class SimpleElasticsearchWriter implements SinkWriter<String> {

        private final String indexName;
        private final RestClient restClient;
        private final RestClientTransport transport;
        private final ElasticsearchClient client;
        private final ObjectMapper objectMapper = new ObjectMapper();

        SimpleElasticsearchWriter(String httpHostAddress, String indexName) {
            this.indexName = indexName;
            HttpHost httpHost = HttpHost.create(httpHostAddress);
            this.restClient = RestClient.builder(httpHost).build();
            this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            this.client = new ElasticsearchClient(transport);
        }

        @Override
        public void write(String element, Context context) throws IOException {
            try {
                JsonNode messageNode = objectMapper.readTree(element);
                String payload = messageNode.has("payload") && !messageNode.get("payload").isNull()
                        ? messageNode.get("payload").asText()
                        : element;

                Map<String, Object> document = new HashMap<>();
                document.put("message", payload);
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
