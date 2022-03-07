package com.zmh.fastlog.worker.file;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.config.FastLogConfig;
import com.zmh.fastlog.model.event.EventSlot;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.AbstractWorker;
import com.zmh.fastlog.worker.mq.MqWorker;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileWorker extends AbstractWorker<ByteData, EventSlot>
    implements SequenceReportingEventHandler<EventSlot>, BatchStartAware, TimeoutHandler {

    private final MqWorker mqWorker;
    private final Disruptor<EventSlot> queue;
    private final RingBuffer<EventSlot> ringBuffer;
    private final FIFOQueue fifo;
    private final int HIGH_WATER_LEVEL_FILE;

    private volatile boolean isClose;

    public FileWorker(MqWorker mqWorker, FastLogConfig config) {
        int batchSize = config.getFileMemoryCacheSize();
        fifo = new FIFOQueue(config.getFileCacheFolder(), batchSize, config.getFileCapacity(), config.getMaxFileCount(), config.getFileCompressType());

        this.mqWorker = mqWorker;
        this.HIGH_WATER_LEVEL_FILE = batchSize;
        queue = new Disruptor<>(
            EventSlot::new,
            batchSize << 2,
            namedDaemonThreadFactory("log-file-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(10, MILLISECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();
    }

    @Override
    public boolean enqueue(ByteData byteData) {
        ringBuffer.publishEvent((e, s) -> byteData.switchData(e.getByteData()));
        return true;
    }

    @Override
    public void dequeue(EventSlot event, long sequence, boolean endOfBatch) {
        fifo.put(event.getByteData());
        event.clear();

        if (notifySeq(sequence) || endOfBatch) {
            onTimeout(sequence);
        }
    }

    @Override
    public void onTimeout(long sequence) {
        ByteData message;
        while (ringBuffer.getCursor() - sequence <= HIGH_WATER_LEVEL_FILE && nonNull(message = fifo.get())) {
            if (isClose || !mqWorker.enqueue(message)) {
                return;
            }
            fifo.next();
        }
    }

    @Override
    public void close() {
        isClose = true;
        queue.shutdown();
        fifo.close();
    }

    private long nextNotifySeq = 0;

    @Override
    public void onBatchStart(long batchSize) {
        nextNotifySeq = 0;
    }

    /**
     * 每发送成功128条日志，则标记ringbuffer该128条日志已消费完成，可以供后续写入，
     * 原因是ringbuffer是批量消费的，每次等批量执行完成之后，再批量标记消费完成，
     * 这样可以防止当ringbuffer一次性消费过多的日志时，可以提前标记消费完成，防止占用太多内存无法供后续写入
     */
    private boolean notifySeq(long currentSeq) {
        long nextNotifySeq = this.nextNotifySeq;
        if (0 == nextNotifySeq) {
            this.nextNotifySeq = currentSeq + 128;
            return false;
        }
        if (currentSeq >= nextNotifySeq) {
            sequenceCallback.set(currentSeq);
            this.nextNotifySeq = currentSeq + 128;
            return true;
        }
        return false;
    }
}


