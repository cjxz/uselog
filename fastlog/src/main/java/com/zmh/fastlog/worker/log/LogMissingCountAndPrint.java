package com.zmh.fastlog.worker.log;

import java.io.Closeable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.LongAdder;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleAtFixedRate;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.sneakyInvoke;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LogMissingCountAndPrint implements Closeable {
    private final String name;

    private long totalMissingCount = 0;
    private final LongAdder logMissingCount = new LongAdder();
    private final ScheduledFuture<?> missingSchedule = scheduleAtFixedRate(this::reportMissCount, 1, 5, SECONDS);

    public LogMissingCountAndPrint(String name) {
        this.name = name;
    }

    public void increment() {
        logMissingCount.increment();
    }

    private void reportMissCount() {
        long sum = logMissingCount.sumThenReset();
        if (sum > 0) {
            totalMissingCount += sum;
            debugLog(name + " log mission count:" + sum + ", total:" + totalMissingCount);
        }
    }

    public long getTotalMissingCount() {
        reportMissCount();
        return totalMissingCount;
    }

    @Override
    public void close() {
        sneakyInvoke(() -> missingSchedule.cancel(true));
    }
}
