package com.zmh.fastlog.worker.mq;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.worker.Worker;
import com.zmh.fastlog.worker.mq.producer.MqProducer;
import lombok.SneakyThrows;

import java.util.concurrent.ScheduledFuture;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleWithFixedDelay;
import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MqWorker implements Worker<AbstractMqMessage>,
    EventHandler<ByteDisruptorEvent>,
    SequenceReportingEventHandler<ByteDisruptorEvent>,
    BatchStartAware,
    TimeoutHandler {

    private final Worker<Object> logWorker;

    private final Disruptor<ByteDisruptorEvent> queue;
    private RingBuffer<ByteDisruptorEvent> ringBuffer;

    private final ScheduledFuture<?> connectFuture;

    private volatile MqProducer producer;

    private volatile boolean isDisposed = false;
    private final int batchSize;

    private boolean isReady;

    public MqWorker(Worker<Object> logWorker, MqProducer producer, int batchSize) {
        this.logWorker = logWorker;
        this.producer = producer;
        this.batchSize = batchSize;

        queue = new Disruptor<>(
            ByteDisruptorEvent::new,
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
        if (producer.connect()) {
            queue.start();
            connectFuture.cancel(true);
        }
    }

    /**
     * mq ring buffer 生产者
     *
     * @param message 入参有两种情况，同一时刻只能有一方会发来日志
     *                1、从文件发过来的 FileMqMessage
     *                2、直接从日志发过来的 LogDisruptorEvent
     * @return true 日志发送成功 false 日志发送失败
     */
    @Override
    public boolean sendMessage(AbstractMqMessage message) {
        return !isDisposed && isReady && ringBuffer.tryPublishEvent((e, s) -> message.apply(e.getByteEvent()));
    }

    private long lastMessageId;

    private long messageCount;

    @Override
    public void onEvent(ByteDisruptorEvent event, long sequence, boolean endOfBatch) {
        long lastMessageId = event.getByteEvent().getId();
        producer.sendEvent(event);

        if (++batchIndex >= batchSize || endOfBatch) { //todo zmh
            producer.flush();
            sequenceCallback.set(sequence);
            batchIndex = 0;
        }
        if (endOfBatch) {
            this.lastMessageId = lastMessageId;
            sendSeqMsg(lastMessageId);
        }
        messageCount++;
    }

    private long nextSendSeqTime;
    private long lastSendSeqId;

    @Override
    public void onTimeout(long sequence) {
        if (sequence < 0) {
            if (!isReady && producer.heartbeat()) {
                isReady = true;
            }
            return;
        }
        sendSeqMsg(sequence);
        if (!isReady && producer.heartbeat()) {
            isReady = true;
        }
    }

    private void sendSeqMsg(long sequence) {
        if (producer.hasMissedMsg()) {
            isReady = false;
        }
        if (lastSendSeqId != lastMessageId || (nextSendSeqTime < currentTimeMillis())) {
            logWorker.sendMessage(new LastConfirmedSeq(lastMessageId));
            nextSendSeqTime = currentTimeMillis() + 1000;
            lastSendSeqId = sequence;
        }
    }

    @Override

    @SneakyThrows
    public void close() {
        debugLog("producer closing...");
        isDisposed = true;
        try {
            queue.shutdown(60, SECONDS);
        } catch (TimeoutException e) {
            debugLog("mq close timeout, force close!");
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
