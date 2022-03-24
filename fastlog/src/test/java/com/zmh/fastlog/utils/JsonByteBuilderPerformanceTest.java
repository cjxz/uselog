package com.zmh.fastlog.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.*;

import static com.zmh.fastlog.utils.Utils.*;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.worker.log.MessageConverter.Consts.*;

public class JsonByteBuilderPerformanceTest {

    private final Calendar calendar = new Calendar.Builder().build();

    private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String text = getPrintText(500);

    private final char[] chars = getText(500).toCharArray();

    @Test
    public void testWriteString() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < 10_0000; j++) {
                chars[i] = (char) i;
                jsonByteBuilder.value(new String(chars)).clear();
            }

            long end = System.currentTimeMillis();

            System.out.println(i + " " + (end - start));
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testStringToByte() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < 10_0000; j++) {
                chars[i] = (char) i;
                byte[] bytes = (new String(chars)).getBytes();
            }

            long end = System.currentTimeMillis();

            System.out.println(i + " " + (end - start));
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void compareDate() {
        List<Integer> integers = Arrays.asList(100, 1000, 1_0000, 10_0000, 100_0000);

        for (Integer num : integers) {
            testDateFormat(num);
            testWriteDate(num);
        }
    }

    //@Test
    public void testWriteDate(int num) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < num; j++) {
                calendar.setTimeInMillis(System.currentTimeMillis());
                jsonByteBuilder.value(calendar).clear();
            }

            //System.out.println(i + " " + (System.currentTimeMillis() - start));
        }

        stopWatch.stop();
        debugLog("auto " + num + " " + stopWatch.formatTime());
    }

    //@Test
    public void testDateFormat(int num) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < num; j++) {
                String format = dateFormat.format(System.currentTimeMillis());
                byte[] bytes = format.getBytes();
            }

            //System.out.println(i + " " + (System.currentTimeMillis() - start));
        }

        stopWatch.stop();
        debugLog("jdk " + num + " " + stopWatch.formatTime());
    }

    @Test
    public void compareJson() {
        List<Integer> integers = Arrays.asList(100, 1000, 1_0000, 10_0000, 100_0000);

        for (Integer num : integers) {
            testJackson(num);
            testJson(num);
        }
    }

    //@Test
    public void testJson(int num) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < num; j++) {
                long currentTimeMillis = System.currentTimeMillis();
                calendar.setTimeInMillis(currentTimeMillis);

                jsonByteBuilder.key(DATA_SEQ).value(j)
                    .key(DATA_MESSAGE).value(text)
                    .key(DATA_LOGGER).value(JsonByteBuilderPerformanceTest.class.getName())
                    .key(DATA_THREAD).value(Thread.currentThread().getName())
                    .key(DATA_LEVEL).value("info")
                    .key(DATA_TIMESTAMP).value(calendar)
                    .key(DATA_TIME_MILLSECOND).value(currentTimeMillis)
                    .clear();
            }

            //System.out.println(i + " " + (System.currentTimeMillis() - start));
        }

        stopWatch.stop();
        debugLog("json " + num + " " + stopWatch.formatTime());
    }

    @SneakyThrows
    //@Test
    public void testJackson(int num) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < num; j++) {
                long currentTimeMillis = System.currentTimeMillis();

                Map<String, Object> maps = new HashMap<>();
                maps.put(DATA_SEQ, j);
                maps.put(DATA_MESSAGE, text);
                maps.put(DATA_LOGGER, JsonByteBuilderPerformanceTest.class.getName());
                maps.put(DATA_THREAD, Thread.currentThread().getName());
                maps.put(DATA_LEVEL, "info");
                maps.put(DATA_TIMESTAMP, dateFormat.format(currentTimeMillis));
                maps.put(DATA_TIME_MILLSECOND, currentTimeMillis);

                String json = objectMapper.writeValueAsString(maps);
                byte[] bytes = json.getBytes();
            }

            //System.out.println(i + " " + (System.currentTimeMillis() - start));
        }

        stopWatch.stop();
        debugLog("jackson " + num + " " + stopWatch.formatTime());
    }

    @Test
    public void testReuse() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < 10_0000; j++) {
                JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
                    .beginObject();

                jsonByteBuilder.value(text);
            }

            long end = System.currentTimeMillis();
            System.out.println(i + " " + (end - start));
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }

    @Test
    public void testNotReuse() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create()
            .beginObject();

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            for (int j = 0; j < 10_0000; j++) {
                jsonByteBuilder.value(text).clear();
            }

            long end = System.currentTimeMillis();
            System.out.println(i + " " + (end - start));
        }

        stopWatch.stop();
        debugLog(stopWatch.formatTime());
    }
}
