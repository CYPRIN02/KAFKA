package org.esgi.project.java.streaming;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class StreamProcessing {
    private final StreamsBuilder builder;

    public StreamProcessing() {
        builder = new StreamsBuilder();
        // TODO: define your topologies & stores
    }

    public Topology buildTopology() {
        return builder.build();
    }
}
