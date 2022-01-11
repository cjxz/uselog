package com.zmh.fastlog.utils;



import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.zmh.fastlog.utils.Utils.debugLog;

/**
 * @author zmh
 */
public class ScheduleUtils {
    private final static ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(5);

    public static ScheduledFuture<?> scheduleAtFixedRate(RethrowRunnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledPool.scheduleAtFixedRate(runWithCatch(command), initialDelay, period, unit);
    }

    public static ScheduledFuture<?> scheduleWithFixedDelay(RethrowRunnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduledPool.scheduleWithFixedDelay(runWithCatch(command), initialDelay, delay, unit);
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
}
