package com.zmh.fastlog.worker.mq.producer;

import org.junit.Test;

public class PulsarProducerTest {

    @Test
    public void testHeartbeat() {
        try (PulsarProducer producer = new PulsarProducer("pulsar://localhost:6650", "persistent://log/test/test-topic", 12)) {
            producer.connect();
            System.out.println(producer.heartbeat());
        }
    }
}
