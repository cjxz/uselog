package com.zmh.fastlog.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static com.zmh.fastlog.utils.Utils.*;
import static com.zmh.fastlog.worker.log.MessageConverter.Consts.*;

public class JsonByteBuilderPerformanceTest {

    private final Calendar calendar = new Calendar.Builder().build();

    private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testWriteString() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 1000_0000; i++) {
            jsonByteBuilder.value(getPrintText(100)).clear();
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testStringToByte() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 1000_0000; i++) {
            byte[] bytes = getPrintText(100).getBytes();
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testWriteDate() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 1000_0000; i++) {
            calendar.setTimeInMillis(System.currentTimeMillis());
            jsonByteBuilder.value(calendar).clear();
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testDateFormat() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 1000_0000; i++) {
            String format = dateFormat.format(System.currentTimeMillis());
            byte[] bytes = format.getBytes();
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testJson() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 1000_0000; i++) {
            long currentTimeMillis = System.currentTimeMillis();
            calendar.setTimeInMillis(currentTimeMillis);

            jsonByteBuilder.key(DATA_SEQ).value(i)
                .key(DATA_MESSAGE).value(getPrintText(100))
                .key(DATA_LOGGER).value(JsonByteBuilderPerformanceTest.class.getName())
                .key(DATA_THREAD).value(Thread.currentThread().getName())
                .key(DATA_LEVEL).value("info")
                .key(DATA_TIMESTAMP).value(calendar)
                .key(DATA_TIME_MILLSECOND).value(currentTimeMillis)
                .clear();
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @SneakyThrows
    @Test
    public void testJackson() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 1000_0000; i++) {
            long currentTimeMillis = System.currentTimeMillis();

            Map<String, Object> maps = new HashMap<>();
            maps.put(DATA_SEQ, i);
            maps.put(DATA_MESSAGE, getPrintText(100));
            maps.put(DATA_LOGGER, JsonByteBuilderPerformanceTest.class.getName());
            maps.put(DATA_THREAD, Thread.currentThread().getName());
            maps.put(DATA_LEVEL, "info");
            maps.put(DATA_TIMESTAMP, dateFormat.format(currentTimeMillis));
            maps.put(DATA_TIME_MILLSECOND, currentTimeMillis);

            byte[] bytes = objectMapper.writeValueAsBytes(maps);
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }
}
