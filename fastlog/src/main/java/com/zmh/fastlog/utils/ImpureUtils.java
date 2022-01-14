package com.zmh.fastlog.utils;


import com.google.common.annotations.VisibleForTesting;

import java.util.function.LongSupplier;

/**
 * 可支持测试的非纯函数库
 */
public final class ImpureUtils {

    private static volatile LongSupplier currentTimeMillisGenerator = TrueCurrentTimeMillisGenerator.INSTANCE;


    public static long currentTimeMillis() {
        return currentTimeMillisGenerator.getAsLong();
    }

    @VisibleForTesting
    public static void setCurrentTimeMillis(long mill) {
        if (mill <= 0) {
            currentTimeMillisGenerator = TrueCurrentTimeMillisGenerator.INSTANCE;
        } else {
            currentTimeMillisGenerator = () -> mill;
        }
    }

    @VisibleForTesting
    static void reset() {
        setCurrentTimeMillis(0);
    }

    static class TrueCurrentTimeMillisGenerator implements LongSupplier {
        static LongSupplier INSTANCE = new TrueCurrentTimeMillisGenerator();

        @Override
        public long getAsLong() {
            return System.currentTimeMillis();
        }
    }
}

