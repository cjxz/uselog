package com.zmh.fastlog.worker.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.util.concurrent.RateLimiter;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.worker.file.FileWorker;
import com.zmh.fastlog.worker.mq.MqWorker;
import lombok.SneakyThrows;
import org.junit.Test;

import java.util.Objects;

import static com.zmh.fastlog.utils.ThreadUtils.sleep;
import static org.apache.pulsar.shade.org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author zmh
 */
public class LogWorkerTest {
    private final LoggerContext loggerContext = new LoggerContext();
    private final Logger logger = loggerContext.getLogger("com.zmh.fastlog.worker");

    private ILoggingEvent getLoggingEvent() {
        return new LoggingEvent(
            "com.zmh.fastlog.worker",
            logger,
            Level.INFO,
            "hello world",
            null,
            null
        );
    }

    @Test
    public void toMqTest() {
        MqWorker mqWorker = mock(MqWorker.class);
        when(mqWorker.enqueue(any())).thenReturn(true);
        FileWorker fileWorker = mock(FileWorker.class);

        try (LogWorker logWorker = new LogWorker(mqWorker, fileWorker, 1024, 1024)) {
            when(fileWorker.enqueue(any()))
                .thenAnswer(msg -> {
                    System.out.println("send message:" + msg.getArgument(0));
                    // mock mq received this message
                    Object e = msg.getArgument(0);
                    if (e instanceof ByteData) {
                        ByteData byteData = (ByteData) e;
                        long id = byteData.getId();
                        System.out.println("sync seq:" + id);
                        logWorker.enqueue(new LastConfirmedSeq(id));
                    }
                    return true;
                });


            ILoggingEvent event = getLoggingEvent();
            logWorker.enqueue(event);

            verify(fileWorker, timeout(500)).enqueue(argThat(Objects::nonNull));
            verify(mqWorker, never()).enqueue(any());
            sleep(100);

            // next message will send to mq
            reset(fileWorker, mqWorker);
            when(fileWorker.enqueue(any())).thenReturn(true);
            when(mqWorker.enqueue(any())).thenReturn(true);
            logWorker.enqueue(event);
            verify(fileWorker, after(100).never()).enqueue(any());
            verify(mqWorker, timeout(100)).enqueue(any());
        }
    }

    @Test
    @SneakyThrows
    public void mqToFileTest() {
        MqWorker mqWorker = mock(MqWorker.class);
        when(mqWorker.enqueue(any())).thenReturn(false);
        FileWorker fileWorker = mock(FileWorker.class);
        when(fileWorker.enqueue(any())).thenReturn(true);

        try (LogWorker logWorker = new LogWorker(mqWorker, fileWorker, 1024, 1024)) {
            writeField(logWorker, "directWriteToMq", true, true);

            for (int i = 0; i < logWorker.getHighWaterLevelMq(); i++) {
                boolean success = logWorker.enqueue(getLoggingEvent());
                assertTrue(success);
            }

            // 达到警戒水位，继续等待
            sleep(500);
            verify(mqWorker, atLeast(1)).enqueue(any());
            verify(fileWorker, never()).enqueue(any());

            // 超过警戒水位，转fileWorker处理
            assertTrue(logWorker.enqueue(getLoggingEvent()));
            verify(fileWorker, timeout(1000).times(logWorker.getHighWaterLevelMq() + 1))
                .enqueue(any());
        }
    }

    @SuppressWarnings({"UnstableApiUsage"})
    @Test
    @SneakyThrows
    public void missingCountTest() {
        RateLimiter limiter = RateLimiter.create(5000);

        FileWorker fileWorker = mock(FileWorker.class);
        when(fileWorker.enqueue(any())).thenReturn(false);

        try (LogWorker logWorker = new LogWorker(mock(MqWorker.class), fileWorker, 128, 1024)) {
            for (int i = 0; i < logWorker.getHighWaterLevelFile(); i++) {
                limiter.acquire();
                boolean success = logWorker.enqueue(getLoggingEvent());
                assertTrue(success);
            }

            assertEquals(0, logWorker.logMissingCount.getTotalMissingCount());

            // 达到警戒水位，继续等待
            sleep(500);

            assertEquals(logWorker.getHighWaterLevelFile() - logWorker.getHighWaterLevelMq(), logWorker.fileMissingCount.getTotalMissingCount());
        }
    }

}
