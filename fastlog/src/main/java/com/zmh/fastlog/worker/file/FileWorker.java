package com.zmh.fastlog.worker.file;

import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceReportingEventHandler;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.ByteDataSoftRef;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.AbstractWorker;
import com.zmh.fastlog.worker.Worker;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileWorker extends AbstractWorker<ByteData, ByteDataSoftRef>
    implements SequenceReportingEventHandler<ByteDataSoftRef>, TimeoutHandler {

    private final Worker<ByteData> mqWorker;
    private final Disruptor<ByteDataSoftRef> queue;
    private final RingBuffer<ByteDataSoftRef> ringBuffer;
    private final FIFOQueue fifo;
    private static final int BUFFER_SIZE = 512;
    private static final int HIGH_WATER_LEVEL_FILE = BUFFER_SIZE >> 1;

    private volatile boolean isClose;

    public FileWorker(Worker<ByteData> mqWorker, int cacheSize, int maxFileCount, String folder) {
        fifo = new FIFOQueue(cacheSize, maxFileCount, folder);

        this.mqWorker = mqWorker;
        queue = new Disruptor<>(
            ByteDataSoftRef::new,
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
    protected boolean enqueue(ByteData byteData) {
        return ringBuffer.tryPublishEvent((e, s) -> byteData.switchData(e.getByteData()));
    }

    @Override
    public void dequeue(ByteDataSoftRef event, long sequence, boolean endOfBatch) {
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
            if (isClose || !mqWorker.sendMessage(message)) {
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


