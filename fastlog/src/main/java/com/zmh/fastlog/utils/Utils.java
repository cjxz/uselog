package com.zmh.fastlog.utils;

import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.*;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.nonNull;

public class Utils {

    private static long lastTime = 0;

    public static void debugLogCondition(String str) {
        long current = currentTimeMillis();
        if (current - 5000 > lastTime) {
            System.out.println(str);
            lastTime = current;
        }
    }

    public static void debugLog(String message) {
        System.out.println(message);
    }

    public static <T extends AutoCloseable> void safeClose(T object) {
        if (nonNull(object)) {
            try {
                object.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void sneakyInvoke(RethrowAction action) {
        if (nonNull(action)) {
            try {
                action.apply();
            } catch (Throwable ignore) {
            }
        }
    }

    public interface RethrowAction {
        void apply() throws Throwable;
    }

    public static String getNowTime() {
        return DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss SSS");
    }


    public static String getText(int size) {
        List<String> stringList = getText1(size);
        Collections.shuffle(stringList);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stringList.size(); i++) {
            sb.append(stringList.get(i));
        }
        return sb.toString();
    }

    public static String getPrintText(int size) {
        StringBuilder sb = new StringBuilder();

        for (int j = 0; j < size; j++) {
            sb.append(RandomStringUtils.randomPrint(1));
        }

        return sb.toString();
    }

    private static List<String> getText1(int size) {
        List<String> list = new ArrayList<>();

        for (int j = 0; j < size / 5; j++) {
            list.add(RandomStringUtils.randomPrint(1));
        }
        for (int j = 0; j < size - size / 5; j++) {
            list.add(getRandomChar());
        }
        return list;
    }

    //随机生成常见汉字
    @SneakyThrows
    private static String getRandomChar() {
        int highCode;
        int lowCode;

        Random random = new Random();

        highCode = (176 + Math.abs(random.nextInt(39))); //B0 + 0~39(16~55) 一级汉字所占区
        lowCode = (161 + Math.abs(random.nextInt(93))); //A1 + 0~93 每区有94个汉字

        byte[] b = new byte[2];
        b[0] = (Integer.valueOf(highCode)).byteValue();
        b[1] = (Integer.valueOf(lowCode)).byteValue();
        return new String(b, "GBK");
    }
}
