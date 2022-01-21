package com.zmh.fastlog.worker.mq;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.log.LogWorker;
import com.zmh.fastlog.worker.mq.producer.KafkaProducer;
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
public class MqWorkerTest {

    @Ignore
    @Test
    @SneakyThrows
    public void onEventTest() {
        LogWorker logWorker = Mockito.mock(LogWorker.class);

        try (MqWorker mqWorker = new MqWorker(new KafkaProducer("", "", 10), 10)) {
            mqWorker.registerLogWorker(logWorker);

            mqWorker.enqueue(new ByteData(10, new byte[13], 10));
            mqWorker.enqueue(new ByteData(11, new byte[13], 10));

            ThreadUtils.sleep(10000);
            assertNotNull(readField(mqWorker, "producer", true));

            verify(logWorker, atLeastOnce()).enqueue(argThat(msg -> ((LastConfirmedSeq) msg).getSeq() == 11));
        }
    }
}