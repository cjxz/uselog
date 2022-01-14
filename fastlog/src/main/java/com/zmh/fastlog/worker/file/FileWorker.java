package com.zmh.fastlog.worker.file;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import com.zmh.fastlog.worker.Worker;
import com.zmh.fastlog.model.event.LogDisruptorEvent;
import com.zmh.fastlog.model.message.AbstractMqMessage;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileWorker implements Worker<LogDisruptorEvent>, SequenceReportingEventHandler<ByteDisruptorEvent>, TimeoutHandler {

    private final Worker<AbstractMqMessage> mqWorker;
    private final Disruptor<ByteDisruptorEvent> queue;
    private final RingBuffer<ByteDisruptorEvent> ringBuffer;
    private final FIFOQueue fifo;
    private static final int BUFFER_SIZE = 512;
    private static final int HIGH_WATER_LEVEL_FILE = BUFFER_SIZE >> 1;

    private volatile boolean isClose;

    public FileWorker(Worker<AbstractMqMessage> mqWorker, int cacheSize, int maxFileCount, String folder) {
        fifo = new FIFOQueue(cacheSize, maxFileCount, folder);

        this.mqWorker = mqWorker;
        queue = new Disruptor<>(
            ByteDisruptorEvent::new,
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
    public boolean sendMessage(LogDisruptorEvent message) {
        return ringBuffer.tryPublishEvent((e, s) -> message.apply(e.getByteEvent()));
    }

    @Override
    public void onEvent(ByteDisruptorEvent event, long sequence, boolean endOfBatch) {
        fifo.put(event.getByteEvent());
        event.clear();

        sequenceCallback.set(sequence);

        if (endOfBatch) {
            onTimeout(sequence);
        }
    }

    @Override
    public void onTimeout(long sequence) {
        AbstractMqMessage message;
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

    private Sequence sequenceCallback;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }
}


