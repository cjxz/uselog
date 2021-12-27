package com.zmh.fastlog.worker;

import com.zmh.fastlog.producer.KafkaEventProducer;
import com.zmh.fastlog.utils.ThreadUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.pulsar.shade.org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@Slf4j
public class PulsarWorkerTest {

    @Ignore
    @Test
    @SneakyThrows
    public void onEventTEST() {
        LogWorker logWorker = Mockito.mock(LogWorker.class);

        try (MqEventWorker pulsarWorker = new MqEventWorker(logWorker, new KafkaEventProducer("", "", 10), 10)) {
            pulsarWorker.sendMessage(new DataByteMessage(10, new byte[13]));
            pulsarWorker.sendMessage(new DataByteMessage(11, new byte[13]));

            ThreadUtils.sleep(10000);
            assertNotNull(readField(pulsarWorker, "producer", true));

            verify(logWorker, atLeastOnce()).sendMessage(argThat(msg -> ((LastSeq) msg).getSeq() == 11));
        }
    }
}