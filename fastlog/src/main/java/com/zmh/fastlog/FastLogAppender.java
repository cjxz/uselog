package com.zmh.fastlog;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.zmh.fastlog.config.FastLogConfig;
import com.zmh.fastlog.worker.file.FileWorker;
import com.zmh.fastlog.worker.log.LogWorker;
import com.zmh.fastlog.worker.mq.MqWorker;
import com.zmh.fastlog.worker.mq.producer.KafkaProducer;
import com.zmh.fastlog.worker.mq.producer.MqProducer;
import com.zmh.fastlog.worker.mq.producer.PulsarProducer;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * @author zmh
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors
public class FastLogAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private Map<String, String> additionalLogFields = new HashMap<>();

    private FastLogConfig config;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private static volatile FastLog fastLog;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private static volatile int instanceCount;

    public FastLogAppender() {
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        if (nonNull(config) && config.isEnable() && nonNull(fastLog)) {
            fastLog.doAppend(eventObject);
        }
    }

    @Override
    public void start() {
        super.start();
        synchronized (FastLogAppender.class) {
            System.out.println("fastlog appender config:" + config);
            instanceCount++;
            if (nonNull(config) && config.isEnable() && isNotBlank(config.getUrl()) && isNotBlank(config.getTopic()) && isNull(fastLog)) {
                System.out.println("fastlog appender created!");
                try {
                    fastLog = new FastLog(config);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        synchronized (FastLogAppender.class) {
            instanceCount--;
            if (nonNull(fastLog) && 0 == instanceCount) {
                System.out.println("fastlog appender stop!");
                fastLog.close();
                fastLog = null;
            }
        }
    }
}

class FastLog implements Closeable {

    private final LogWorker logWorker;
    private final MqWorker mqWorker;
    private final FileWorker fileWorker;

    public FastLog(FastLogConfig config) {
        try {
            MqProducer producer;
            if ("pulsar".equals(config.getMqType())) {
                producer = new PulsarProducer(config.getUrl(), config.getTopic(), config.getBatchSize());
            } else {
                producer = new KafkaProducer(config.getUrl(), config.getTopic(), config.getBatchSize());
            }
            mqWorker = new MqWorker(producer, config.getBatchMessageSize());
            fileWorker = new FileWorker(mqWorker, config.getBatchMessageSize(), config.getFileMemoryCacheSize(), config.getFileMaxCacheCount(), config.getMaxFileCount(), config.getFileCacheFolder());
            logWorker = new LogWorker(mqWorker, fileWorker, config.getBatchMessageSize(), config.getMaxMsgSize());
        } catch (Exception ex) {
            this.close();
            throw ex;
        }
    }

    public void doAppend(Object message) {
        logWorker.enqueue(message);
    }

    @Override
    public void close() {
        mqWorker.close();
        logWorker.close();
        fileWorker.close();
    }
}


