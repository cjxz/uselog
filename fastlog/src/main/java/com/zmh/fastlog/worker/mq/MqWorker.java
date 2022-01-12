package com.zmh.fastlog.worker.mq;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.Worker;
import com.zmh.fastlog.worker.mq.producer.MqProducer;
import lombok.SneakyThrows;

import java.nio.ByteBuffer;

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

    private volatile MqProducer mqProducer;

    private volatile boolean isDisposed = false;

    private final int batchSize;

    public MqWorker(Worker<Object> logWorker, MqProducer mqProducer, int batchSize) {
        this.logWorker = logWorker;
        this.mqProducer = mqProducer;
        this.batchSize = batchSize;

        queue = new Disruptor<>(
            ByteDisruptorEvent::new,
            batchSize << 2,
            namedDaemonThreadFactory("log-mq-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(1, SECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();

        mqProducer.connect();
        queue.start();
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
        return !isDisposed && mqProducer.isReady() && ringBuffer.tryPublishEvent((e, s) -> message.apply(e.getByteEvent()));
    }

    // 上次mq成功发送出去的messageId
    private long lastMessageId;

    @Override
    public void onEvent(ByteDisruptorEvent event, long sequence, boolean endOfBatch) {
        // 消费的时候，有可能mqProducer还没准备好，此时需要尽可能的等待mqProducer准备好为止
        while (!mqProducer.isReady()) {
            ThreadUtils.sleep(100); // todo zmh ??
        }

        long processMessageId = event.getByteEvent().getId();

        if (processMessageId == 0L) {
            ByteBuffer buffer = event.getByteEvent().getBuffer();
            System.out.println("lastMessageId " + processMessageId + " position " + buffer.position() + " limit" + buffer.limit() + " size" + buffer.capacity());
        }
        mqProducer.sendEvent(event);

        if (++batchIndex >= batchSize || endOfBatch) { //todo zmh
            mqProducer.flush();
            sequenceCallback.set(sequence);
            batchIndex = 0;
            this.lastMessageId = processMessageId;
        }
        if (endOfBatch) {
            sendSeqMsg();
        }
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

    @Override
    public void onTimeout(long sequence) {
        if (sequence < 0) {
            return;
        }
        sendSeqMsg();
    }

    // 下次发送messageId给log的时间 不早于这个时间，限制时间为了防止在日志低频发送时，不会每发一条日志，就通知一下logworker
    private long nextSendSeqTime;
    // 上次发送给logworker，mq已经成功处理的messageId
    private long lastSendSeqId;

    /**
     * 发送给logworker，mq已经成功处理的messageId
     * 真正触发条件：
     * 1、当上次发送给logworker的messageId跟当前mq成功发送出去的messageId不一样时 or
     * 2、时间晚于下次发送时间，这里会重复发送给logworker同样的messageId，目的是为了防止logworker有消息丢失的现象
     */
    private void sendSeqMsg() {
        if (lastSendSeqId != lastMessageId || (nextSendSeqTime < currentTimeMillis())) {
            logWorker.sendMessage(new LastConfirmedSeq(lastMessageId));
            nextSendSeqTime = currentTimeMillis() + 1000;
            lastSendSeqId = lastMessageId;
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
        mqProducer.close();
    }
}
