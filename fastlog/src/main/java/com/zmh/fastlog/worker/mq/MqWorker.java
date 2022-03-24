package com.zmh.fastlog.worker.mq;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.EventSlot;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.AbstractWorker;
import com.zmh.fastlog.worker.log.LogMissingCountAndPrint;
import com.zmh.fastlog.worker.log.LogWorker;
import com.zmh.fastlog.worker.mq.producer.MqProducer;
import lombok.SneakyThrows;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MqWorker extends AbstractWorker<ByteData, EventSlot>
    implements BatchStartAware, TimeoutHandler {

    private LogWorker logWorker;

    private final Disruptor<EventSlot> queue;
    private RingBuffer<EventSlot> ringBuffer;

    private volatile MqProducer mqProducer;

    private volatile boolean isDisposed = false;

    private final int batchSize;
    private LogMissingCountAndPrint mqCount = new LogMissingCountAndPrint("mq send count");


    public MqWorker(MqProducer mqProducer, int batchSize) {
        this.mqProducer = mqProducer;
        this.batchSize = batchSize;

        queue = new Disruptor<>(
            EventSlot::new,
            batchSize << 4,
            namedDaemonThreadFactory("log-mq-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(1, SECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();

        mqProducer.connect();
        queue.start();
    }

    public void registerLogWorker(LogWorker logWorker) {
        this.logWorker = logWorker;
    }

    /**
     * mq ring buffer 生产者
     *
     * @param byteData 入参有两种情况，同一时刻只能有一方会发来日志
     *                1、从文件发过来的
     *                2、直接从日志发过来的
     * @return true 日志发送成功 false 日志发送失败
     */
    @Override
    public boolean enqueue(ByteData byteData) {
        return !isDisposed && mqProducer.isReady() && ringBuffer.tryPublishEvent((e, s) -> byteData.switchData(e.getByteData()));
    }

    // 上次mq成功发送出去的messageId
    private long lastMessageId;

    @Override
    public void dequeue(EventSlot event, long sequence, boolean endOfBatch) {
        // 消费的时候，有可能mqProducer还没准备好，此时需要尽可能的等待mqProducer准备好为止
        while (!mqProducer.isReady()) {
            ThreadUtils.sleep(100);
        }

        long processMessageId = event.getByteData().getId();

        mqProducer.sendEvent(event);
        mqCount.increment();

        if (++batchIndex >= batchSize || endOfBatch) {
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
            logWorker.enqueue(new LastConfirmedSeq(lastMessageId));
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
        mqCount.close();
    }
}
