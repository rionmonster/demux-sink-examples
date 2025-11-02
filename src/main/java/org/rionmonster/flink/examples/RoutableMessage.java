package org.rionmonster.flink.examples;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

/**
 * A simple message class that contains routing information.
 * This can be used to test the DemultiplexingSink by routing messages
 * to different destinations based on the topic field.
 */
public record RoutableMessage(
    @JsonProperty("topic") String topic,
    @JsonProperty("payload") String payload
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String payload() {
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
        if (!(o instanceof RoutableMessage that)) return false;
        return topic.equals(that.topic) && payload.equals(that.payload);
    }

}

