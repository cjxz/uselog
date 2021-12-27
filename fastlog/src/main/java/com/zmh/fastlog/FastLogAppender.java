package com.zmh.fastlog;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.zmh.fastlog.config.FastLogConfig;
import com.zmh.fastlog.worker.Worker;
import lombok.*;
import lombok.experimental.Accessors;

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
    private static volatile Worker<Object> wrapper;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private static volatile int instanceCount;

    public FastLogAppender() {
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        if (nonNull(config) && config.isEnable() && nonNull(wrapper)) {
            wrapper.sendMessage(eventObject);
        }
    }

    @Override
    public void start() {
        super.start();
        synchronized (FastLogAppender.class) {
            System.out.println("fastlog appender config:" + config);
            instanceCount++;
            if (nonNull(config) && config.isEnable() && isNotBlank(config.getUrl()) && isNotBlank(config.getTopic()) && isNull(wrapper)) {
                System.out.println("fastlog appender created!");
                try {
                    wrapper = FastLogAppenderFactory.create(config);
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
            if (nonNull(wrapper) && 0 == instanceCount) {
                System.out.println("fastlog appender stop!");
                wrapper.close();
                wrapper = null;
            }
        }
    }
}


