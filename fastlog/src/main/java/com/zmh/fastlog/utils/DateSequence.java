package com.zmh.fastlog.utils;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.currentTimeMillis;

public class DateSequence {

    private final static long MILL_SECOND_OFFSET = 1546300800000L;
    /**
     * 最后一个生成的序列编号
     *
     * 0,    millSec,    bizId,      seq
     * 1bit, 41bit,      10bit,      12bit
     * 0,    [0, now()], [0, 1024],  [0, 4095]
     * 41位的时间序列，精确到毫秒，从2019-01-01起,可以用至2089-01-01年
     * 10位的机器标识，最多支持部署1024个节点
     * 12位的序列号，支持每个节点每毫秒产生4096个ID序号，最高位是符号位始终为0。
     */
    private final AtomicLong last = new AtomicLong(0);
    private final long bizId;

    public DateSequence() {
        this(0);
    }

    public DateSequence(int bizId) {
        if (bizId < 0 || bizId >= 1024) {
            throw new IllegalArgumentException(String.format("bizId should in range [0, 1024), but %d", bizId));
        }
        this.bizId = ((long) bizId) << 12;
    }

    public static int getBizId(long dateSeq) {
        return (int) (dateSeq >>> 12) & 0x02FF;
    }

    public static long getMillSec(long dateSeq) {
        return (dateSeq >>> 22) + MILL_SECOND_OFFSET;
    }

    private static long getMillPart(long dateSeq) {
        return (dateSeq >>> 22);
    }

    public static long getSeq(long dateSeq) {
        return dateSeq & 0x0FFF;
    }

    public static long getCurrentTimeMills() {
        // offset 2019-01-01
        return currentTimeMillis() - MILL_SECOND_OFFSET;
    }

    public static String toString(long dateSeq) {
        int bizId = getBizId(dateSeq);
        long mill = getMillSec(dateSeq);
        long seq = getSeq(dateSeq);
        return String.format("(%d, %d, %d)", mill, bizId, seq);
    }

    /**
     * 当前时间的毫秒数+序号, 同一毫秒内的序号(低16bit)为8bit(0-65535)
     */
    public long next() {
        return next(1);
    }

    public long next(int batchSize) {
        if (batchSize < 1) {
            batchSize = 1;
        }
        long newMill = getCurrentTimeMills();
        final long bizId = this.bizId;
        final AtomicLong last = this.last;
        for (; ; ) {
            long newSeq;
            long prev = last.get();
            long prevSeq = getSeq(prev);
            long prevMill = getMillPart(prev);
            if (prevMill < newMill) {
                newSeq = batchSize;
            } else if (prevMill == newMill) {
                newSeq = prevSeq + batchSize;
                if (newSeq == 4096) {
                    newMill = newMill + 1;
                    continue;
                }
            } else { // prevMill > newMill
                newMill = prevMill;
                newSeq = prevSeq + batchSize;
            }

            // overflow
            long mill = newMill + (newSeq >>> 12);
            long curt = (mill << 22) | bizId | (newSeq & 0x0FFF);
            if (last.compareAndSet(prev, curt)) {
                return curt;
            }
        }
    }

}
