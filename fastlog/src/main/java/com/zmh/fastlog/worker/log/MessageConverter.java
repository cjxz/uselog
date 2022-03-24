package com.zmh.fastlog.worker.log;

import ch.qos.logback.classic.pattern.CallerDataConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.JsonByteBuilder;

import java.util.Calendar;
import java.util.Map;

import static com.zmh.fastlog.worker.log.MessageConverter.Consts.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class MessageConverter {

    private final ThreadLocal<JsonByteBuilder> threadLocal = new ThreadLocal<>();

    private final Calendar calendar = new Calendar.Builder().build();

    //最大的日志长度，单位字节，大于这个长度截取
    private final int maxMsgSize;

    public MessageConverter(int maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }

    public void convertToByteData(ILoggingEvent log, ByteData byteData, long sequence) {
        JsonByteBuilder jsonByteBuilder = getJsonByteBuilder();

        jsonByteBuilder.clear()
            .beginObject(byteData.getData())
            .key(DATA_SEQ).value(sequence)
            .key(DATA_MESSAGE).value(log.getFormattedMessage(), maxMsgSize)
            .key(DATA_LOGGER).value(log.getLoggerName())
            .key(DATA_THREAD).value(log.getThreadName())
            .key(DATA_LEVEL).value(log.getLevel().levelStr);

        long timeStamp = log.getTimeStamp();
        calendar.setTimeInMillis(timeStamp);
        jsonByteBuilder
            .key(DATA_TIMESTAMP).value(calendar)
            .key(DATA_TIME_MILLSECOND).value(timeStamp);

        if (nonNull(log.getMarker())) {
            jsonByteBuilder
                .key(DATA_MARKER)
                .value(log.getMarker().toString());
        }
        if (log.hasCallerData()) {
            jsonByteBuilder
                .key(DATA_CALLER)
                .value(new CallerDataConverter().convert(log));
        }
        if (nonNull(log.getThrowableProxy())) {
            jsonByteBuilder
                .key(DATA_THROWABLE)
                .value(ThrowableProxyUtil.asString(log.getThrowableProxy()));
        }
        Map<String, String> mdc = log.getMDCPropertyMap();
        if (mdc.size() > 0) {
            mdc.forEach((k, v) -> jsonByteBuilder.key(k).value(v));
        }

        jsonByteBuilder.endObject();

        // 有可能data发生了扩容
        byteData.setData(jsonByteBuilder.array());
        byteData.setDataLength(jsonByteBuilder.pos());
    }

    /**
     * 从当前线程中获取，避免多线程并发问题
     */
    private JsonByteBuilder getJsonByteBuilder() {
        JsonByteBuilder jsonByteBuilder = this.threadLocal.get();

        if (isNull(jsonByteBuilder)) {
            jsonByteBuilder = JsonByteBuilder.create();
            threadLocal.set(jsonByteBuilder);
        }
        return jsonByteBuilder;
    }

    @SuppressWarnings("unused")
    public static class Consts {
        public static final String DATA_MESSAGE = "message";
        public static final String DATA_LOGGER = "logger";
        public static final String DATA_THREAD = "thread";
        public static final String DATA_LEVEL = "level";
        public static final String DATA_MARKER = "marker"; //todo
        public static final String DATA_CALLER = "caller"; //todo
        public static final String DATA_SEQ = "seq";
        public static final String DATA_THROWABLE = "throwable";
        public static final String DATA_TIME_MILLSECOND = "ts";
        public static final String DATA_TIMESTAMP = "@timestamp";
    }
}
