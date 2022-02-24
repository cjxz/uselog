package com.zmh.fastlog.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

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

    public static int marginToBuffer(int len) {
        if ((len & 63) != 0) {
            len &= ~63;
            len += 64;
        }
        return len;
    }

}
