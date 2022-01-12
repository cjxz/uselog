package com.zmh.fastlog.worker.mq.producer;

import org.junit.Test;

public class KafkaProducerTest {

    @Test
    public void testHeartbeat() {
        try (KafkaProducer producer = new KafkaProducer("10.106.112.57:9092", "log4", 12)) {
            producer.connect();
        }
    }
}
