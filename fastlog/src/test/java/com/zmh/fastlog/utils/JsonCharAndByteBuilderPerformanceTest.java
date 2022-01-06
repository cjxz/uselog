package com.zmh.fastlog.utils;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.nio.ByteBuffer;

public class JsonCharAndByteBuilderPerformanceTest {


    @Test
    public void performanceCharTest() {
        StopWatch stopWatch = new StopWatch();
        String value = getText(100);

        ByteBuffer allocate = ByteBuffer.allocate(512);
        JsonCharBuilder jsonCharBuilder = JsonCharBuilder.create();
        yure(jsonCharBuilder);
        stopWatch.start();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10_0000; j++) {
                JsonCharBuilder byteBuilder = jsonCharBuilder
                    .clear()
                    .beginObject()
                    .key("threadName").value(value)
                    .key("env").value(value)
                    .key("@timestamp").value(value)
                    .key("applicationName").value(value)
                    .key("msg").value(value)
                    .endObject();

                byteBuilder.toByteBuffer(allocate);
            }
        }
        stopWatch.stop();
        System.out.println(stopWatch.formatTime());
    }

    @Test
    public void performanceByteTest() {
        StopWatch stopWatch = new StopWatch();
        String value = getText(100);

        JsonByteBuilder jsonByteBuilder = JsonByteBuilder.create(1024);
        yure1(jsonByteBuilder);
        stopWatch.start();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10_0000; j++) {
                jsonByteBuilder
                    .clear()
                    .beginObject()
                    .key("threadName").value(value)
                    .key("env").value(value)
                    .key("@timestamp").value(value)
                    .key("applicationName").value(value)
                    .key("msg").value(value)
                    .endObject();
            }
        }
        stopWatch.stop();
        System.out.println(stopWatch.formatTime());
    }

    private void yure1(JsonByteBuilder jsonByteBuilder) {
        String value = getText(100);

        for (int i = 0; i < 1_0000; i++) {
            JsonByteBuilder byteBuilder = jsonByteBuilder
                .clear()
                .beginObject()
                .key("threadName").value(value)
                .key("env").value(value)
                .key("@timestamp").value(value)
                .key("applicationName").value(value)
                .key("msg").value(value)
                .endObject();
        }
    }

    private void yure(JsonCharBuilder jsonCharBuilder) {
        ByteBuffer allocate = ByteBuffer.allocate(512);

        String value = getText(100);

        for (int i = 0; i < 1_0000; i++) {
            JsonCharBuilder byteBuilder = jsonCharBuilder
                .clear()
                .beginObject()
                .key("threadName").value(value)
                .key("env").value(value)
                .key("@timestamp").value(value)
                .key("applicationName").value(value)
                .key("msg").value(value)
                .endObject();

            byteBuilder.toByteBuffer(allocate);
        }
    }

    private String getText(int size) {
        String str = "abcdefghijkelmnopqrstuvwsyzABCDEFGHIJKLMNOPQRSTUVWSYZ1234567890!@#$%^&*()_+=\\在并发量比较大的情况下，操作数据的时候，相当于把这个数字分成了很多份数字，然后交给多个人去管控，每个管控者负责保证部分数字在多线程情况下操作的正确性";
        int length = str.length();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            int num = RandomUtils.nextInt(0, length);
            sb.append(str.charAt(num));
        }
        sb.append("。");
        return sb.toString();
    }
}
