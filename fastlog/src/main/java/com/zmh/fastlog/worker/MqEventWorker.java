package com.zmh.fastlog.worker;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.producer.MqEvent;
import com.zmh.fastlog.producer.MqEventProducer;

import java.util.concurrent.ScheduledFuture;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleWithFixedDelay;
import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MqEventWorker implements Worker<Object>,
    EventHandler<MqEvent>,
    SequenceReportingEventHandler<MqEvent>,
    BatchStartAware,
    TimeoutHandler {

    private final Worker<Object> logWorker;

    private final Disruptor<MqEvent> queue;
    private RingBuffer<MqEvent> ringBuffer;

    private final ScheduledFuture<?> connectFuture;

    private volatile MqEventProducer producer;

    private volatile boolean isDisposed = false;
    private final int batchSize;

    public MqEventWorker(Worker<Object> logWorker, MqEventProducer producer, int batchSize) {
        this.logWorker = logWorker;
        this.producer = producer;
        this.batchSize = batchSize;

        queue = new Disruptor<>(
            MqEvent::new,
            batchSize << 1,
            namedDaemonThreadFactory("log-mq-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(1, SECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();

        connectFuture = scheduleWithFixedDelay(this::connect, 0, 5, SECONDS);
    }

    private void connect() {
        boolean success = producer.connect();
        if (success) {
            queue.start();
        }
        connectFuture.cancel(success);
    }

    @Override
    public boolean sendMessage(Object message) {
        ByteMessage msg = (ByteMessage) message;
        return !isDisposed && ringBuffer.tryPublishEvent((e, s) -> {
            ByteEvent byteEvent = e.getByteEvent();
            msg.apply(byteEvent);
        });
    }

    private long lastMessageId;

    private long messageCount;

    @Override
    public void onEvent(MqEvent event, long sequence, boolean endOfBatch) {
        ByteEvent byteEvent = event.getByteEvent();
        long lastMessageId = byteEvent.getId();
        //debugLog("receive event: " + lastMessageId + "," + endOfBatch + "," + event.getEventString());

        int eventSize = byteEvent.getBufferLen();
        if (eventSize > 1_000_000) {
            debugLog("日志过大, 直接丢弃:" + event.getEventString());
        } else {
            producer.sendEvent(event);
        }
        if (++batchIndex >= batchSize || endOfBatch) { //todo zmh
            producer.flush();
            sequenceCallback.set(sequence);
            //debugLog("mq size:" + (ringBuffer.getCursor() - sequence));
            batchIndex = 0;
        }
        if (endOfBatch) {
            this.lastMessageId = lastMessageId;
            onTimeout(lastMessageId);
        }
        messageCount++;
    }

    private long nextSendSeqTime;
    private long lastSendSeqId;

    @Override
    public void onTimeout(long sequence) {
        if (sequence < 0) {
            return;
        }
        if (lastSendSeqId != lastMessageId || (nextSendSeqTime < currentTimeMillis())) {
            final long l = currentTimeMillis();
            //debugLog("mq sync seq:" + lastMessageId + "," + l);
            logWorker.sendMessage(new LastSeq(lastMessageId));
            nextSendSeqTime = l + 1000;
            lastSendSeqId = sequence;
        }
    }

    @Override
    public void close() {
        debugLog("producer closing...");
        isDisposed = true;
        connectFuture.cancel(true);
        try {
            queue.shutdown(60, SECONDS);
        } catch (TimeoutException e) {
            debugLog("pulsar close timeout, force close!");
            queue.halt();
        }
        producer.close();
        debugLog("producer closed, total message count:" + messageCount);
    }

    private int batchIndex;

    @Override
    public void onBatchStart(long batchSize) {
        batchIndex = 0;
    }

    private Sequence sequenceCallback;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }

}
