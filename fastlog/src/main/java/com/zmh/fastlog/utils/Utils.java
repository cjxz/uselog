package com.zmh.fastlog.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.Objects;

import static java.util.Objects.nonNull;

public class Utils {

    public static void debugLog(String message) {
        System.out.println(message);
    }

    public static <T extends AutoCloseable> T safeClose(T object) {
        if (Objects.nonNull(object)) {
            try {
                object.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return object;
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

    public static Runnable runWithCatch(RethrowRunnable fun) {
        return () -> {
            try {
                fun.run();
            } catch (Throwable e) {
                debugLog("execute error, but sneaky catch");
                e.printStackTrace();
            }
        };
    }

    public interface RethrowRunnable {
        void run() throws Throwable;
    }

    public static String getNowTime() {
        return DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss SSS");
    }
}
