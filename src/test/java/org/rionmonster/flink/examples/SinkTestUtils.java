package org.rionmonster.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class SinkTestUtils {

    public static StreamExecutionEnvironment createTestStreamEnvironment() {
        var streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        return streamEnv;
    }

    public static void executeAndWait(StreamExecutionEnvironment env, long delayMs) throws Exception {
        env.execute();
        Thread.sleep(delayMs);
    }
}
