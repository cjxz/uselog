package com.zmh.fastlog.worker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import lombok.SneakyThrows;
import org.junit.Test;

import static com.zmh.fastlog.utils.ThreadUtils.sleep;
import static org.apache.pulsar.shade.org.apache.commons.lang3.reflect.FieldUtils.writeField;
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

    @SuppressWarnings("unchecked")
    @Test
    public void toPulsarTest() {
        Worker<Object> pulsarWorker = mock(Worker.class);
        when(pulsarWorker.sendMessage(any())).thenReturn(true);
        Worker<Object> fileWorker = mock(Worker.class);

        try (LogWorker logWorker = new LogWorker(pulsarWorker, fileWorker, 1024,null)) {
            when(fileWorker.sendMessage(any()))
                .thenAnswer(msg -> {
                    System.out.println("send message:" + msg.getArgument(0));
                    // mock pulsar received this message
                    Object e = msg.getArgument(0);
                    if (e instanceof ByteMessage) {
                        ByteMessage byteMessage = (ByteMessage) e;
                        long id = byteMessage.getId();
                        System.out.println("sync seq:" + id);
                        logWorker.sendMessage(new LastSeq(id));
                    }
                    return true;
                });


            ILoggingEvent event = getLoggingEvent();
            logWorker.sendMessage(event);

            verify(fileWorker, timeout(500)).sendMessage(argThat(msg -> msg instanceof ByteMessage));
            verify(pulsarWorker, never()).sendMessage(any());
            sleep(100);

            // next message will send to pulsar
            reset(fileWorker, pulsarWorker);
            when(fileWorker.sendMessage(any())).thenReturn(true);
            when(pulsarWorker.sendMessage(any())).thenReturn(true);
            logWorker.sendMessage(event);
            verify(fileWorker, after(100).never()).sendMessage(any());
            verify(pulsarWorker, timeout(100)).sendMessage(any());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @SneakyThrows
    public void pulsarToFileTest() {
        Worker<Object> pulsarWorker = mock(Worker.class);
        when(pulsarWorker.sendMessage(any())).thenReturn(false);
        Worker<Object> fileWorker = mock(Worker.class);
        when(fileWorker.sendMessage(any())).thenReturn(true);

        try (LogWorker logWorker = new LogWorker(pulsarWorker, fileWorker, 1024, null)) {
            writeField(logWorker, "directWriteToPulsar", true, true);

            for (int i = 0; i < logWorker.getHighWaterLevelPulsar(); i++) {
                boolean success = logWorker.sendMessage(getLoggingEvent());
                assertTrue(success);
            }

            // 达到警戒水位，继续等待
            sleep(500);
            verify(pulsarWorker, atLeast(1)).sendMessage(any());
            verify(fileWorker, never()).sendMessage(any());

            // 超过警戒水位，转fileWorker处理
            assertTrue(logWorker.sendMessage(getLoggingEvent()));
            verify(fileWorker, timeout(1000).times(logWorker.getHighWaterLevelPulsar() + 1))
                .sendMessage(any());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    @SneakyThrows
    public void missingCountTest() {
        Worker<Object> pulsarWorker = mock(Worker.class);
        when(pulsarWorker.sendMessage(any())).thenReturn(false);
        Worker<Object> fileWorker = mock(Worker.class);
        when(fileWorker.sendMessage(any())).thenReturn(false);

        try (LogWorker logWorker = new LogWorker(pulsarWorker, fileWorker, 1024,null)) {
            writeField(logWorker, "directWriteToPulsar", true, true);

            for (int i = 0; i < logWorker.getHighWaterLevelFile(); i++) {
                boolean success = logWorker.sendMessage(getLoggingEvent());
                assertTrue(success);
            }

            // 达到警戒水位，继续等待
            sleep(500);

        }
    }

}
