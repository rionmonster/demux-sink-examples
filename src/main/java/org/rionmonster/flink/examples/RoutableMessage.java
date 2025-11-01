package org.rionmonster.flink.examples;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * A simple message class that contains routing information.
 * This can be used to test the DemultiplexingSink by routing messages
 * to different destinations based on the topic field.
 */
public class RoutableMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("topic")
    private final String topic;

    @JsonProperty("payload")
    private final String payload;

    public RoutableMessage(
            @JsonProperty("topic") String topic,
    @JsonProperty("payload") String payload) {
        this.topic = topic;
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "RoutableMessage{" +
                "topic='" + topic + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RoutableMessage)) return false;
        RoutableMessage that = (RoutableMessage) o;
        return topic.equals(that.topic) && payload.equals(that.payload);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + payload.hashCode();
        return result;
    }
}

