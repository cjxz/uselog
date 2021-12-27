package com.zmh.fastlog.utils;


import com.zmh.fastlog.utils.Utils.RethrowRunnable;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.zmh.fastlog.utils.Utils.runWithCatch;

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
}
