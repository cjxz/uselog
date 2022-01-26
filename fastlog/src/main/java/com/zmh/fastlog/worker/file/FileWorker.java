package com.zmh.fastlog.worker.file;

import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceReportingEventHandler;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.EventSlot;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.AbstractWorker;
import com.zmh.fastlog.worker.mq.MqWorker;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileWorker extends AbstractWorker<ByteData, EventSlot>
    implements SequenceReportingEventHandler<EventSlot>, TimeoutHandler {

    private final MqWorker mqWorker;
    private final Disruptor<EventSlot> queue;
    private final RingBuffer<EventSlot> ringBuffer;
    private final FIFOQueue fifo;
    private static final int BUFFER_SIZE = 512;
    private static final int HIGH_WATER_LEVEL_FILE = BUFFER_SIZE >> 1;

    private volatile boolean isClose;

    public FileWorker(MqWorker mqWorker, int cacheSize, int fileMaxCacheCount, int maxFileCount, String folder) {
        fifo = new FIFOQueue(folder, cacheSize, fileMaxCacheCount, maxFileCount);

        this.mqWorker = mqWorker;
        queue = new Disruptor<>(
            EventSlot::new,
            BUFFER_SIZE,
            namedDaemonThreadFactory("log-file-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(100, MILLISECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();
    }

    @Override
    public boolean enqueue(ByteData byteData) {
        return ringBuffer.tryPublishEvent((e, s) -> byteData.switchData(e.getByteData()));
    }

    @Override
    public void dequeue(EventSlot event, long sequence, boolean endOfBatch) {
        fifo.put(event.getByteData());
        event.clear();

        sequenceCallback.set(sequence);

        if (endOfBatch) {
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
}


