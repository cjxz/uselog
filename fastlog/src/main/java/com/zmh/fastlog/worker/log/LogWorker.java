package com.zmh.fastlog.worker.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.lmax.disruptor.BatchStartAware;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.EventSlot;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.AbstractWorker;
import com.zmh.fastlog.worker.file.FileWorker;
import com.zmh.fastlog.worker.mq.MqWorker;
import lombok.Getter;
import lombok.val;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getNowTime;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.startsWithAny;

/**
 * @author zmh
 */
public class LogWorker extends AbstractWorker<Object, EventSlot>
    implements BatchStartAware {

    // 日志有两个可能方向, 一个往mq, 一个写本地文件缓存
    // 以下两个是高水位阈值, 日志堆积超过这个阈值后应该丢弃之前的日志,
    // 这种策略同时能达到两个目地:
    //   1. 缓冲区空间足够的时候尽可能的等后续渠道消费日志, 空间紧张时预先丢弃不重要的日志
    //   2. 日志和同步SEQ都是通过消息发送, 但消息优先级应该更高, 通过丢弃之前的同步SEQ消息使得消息能更快被处理
    @Getter
    private final int highWaterLevelMq;
    @Getter
    private final int highWaterLevelFile;

    // 日志缓冲区
    private final Disruptor<EventSlot> queue;
    private final RingBuffer<EventSlot> ringBuffer;

    // 统计丢弃的日志数
    final LogMissingCountAndPrint logMissingCount = new LogMissingCountAndPrint("log");
    final LogMissingCountAndPrint fileMissingCount = new LogMissingCountAndPrint("file");

    // 消息去向, 二选1
    // 初始时先通过file,file缓冲区为空的切到mq
    // mq堵塞的时候切到file缓存
    private boolean directWriteToMq = false;
    private final MqWorker mqWorker;
    private final FileWorker fileWorker;

    // 日志序列化类
    private MessageConverter messageConverter;

    private volatile boolean isClosed = false;

    public LogWorker(MqWorker mqWorker, FileWorker fileWorker, int batchSize, int maxMsgSize) {
        this.messageConverter = new MessageConverter(maxMsgSize);
        this.mqWorker = mqWorker;
        this.fileWorker = fileWorker;
        // 缓冲区设置
        // 初始的缓冲池, 避免短期内日志突然增多造成日志来不及处理而丢失
        // 本实例是日志的入口, 尽量通过缓冲区把各个线程的日志的平缓的收集过来
        int bufferSize = batchSize << 4;
        this.highWaterLevelFile = (int) (bufferSize * 0.75);
        this.highWaterLevelMq = bufferSize >> 1;

        queue = new Disruptor<>(
            EventSlot::new,
            bufferSize,
            namedDaemonThreadFactory("log-log-worker"),
            ProducerType.MULTI, // 注意此处为多生产者
            new LiteBlockingWaitStrategy()
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();

        mqWorker.registerLogWorker(this);
    }

    /**
     * log ring buffer 生产者
     *
     * @param message 入参有两种类型，1、正常日志 ILoggingEvent  2、mq发过来的已消费序号 LastSeq
     * @return true 日志发送成功 false 日志发送失败
     */
    @SuppressWarnings("CodeBlock2Expr")
    @Override
    public boolean enqueue(Object message) {
        if (message instanceof ILoggingEvent) {
            val msg = (ILoggingEvent) message;
            if (isExclude(msg)) {
                return true;
            }
            if (!ringBuffer.tryPublishEvent((event, sequence) -> {
                messageConverter.convertToByteData(msg, event.getByteData(), sequence);
            })) {
                logMissingCount.increment();
                return false;
            } else {
                return true;
            }
        } else if (message instanceof LastConfirmedSeq) {
            long lastSeq = ((LastConfirmedSeq) message).getSeq();
            // 本地文件缓冲区已经发完了, 后续日志切换到mq
            if (lastSeq == lastMessageId && !directWriteToMq) {
                directWriteToMq = true;
                debugLog("本地cache已经清空,切换到mq," + getNowTime());
            }
            return true;
        }
        return false;
    }

    // 日志id, 发送成功一条加1, 用于识别每条日志，方便后续切换mq使用
    private long lastMessageId = 0;

    // ringbuffer的消费者逻辑，这里已经是单线程了，lastMessageId没有并发问题
    @Override
    public void dequeue(EventSlot event, long sequence, boolean endOfBatch) {
        long messageId = lastMessageId + 1;

        ByteData byteData = event.getByteData();
        byteData.setId(messageId);

        boolean success = false;
        if (directWriteToMq) {
            while (!(success = mqWorker.enqueue(byteData))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelMq) {
                    break;
                }
                ThreadUtils.sleep(5);
            }
            // 写入失败, 切换到本地文件缓冲区
            if (!success && directWriteToMq) {
                directWriteToMq = false;
                debugLog("mq阻塞,切换到file cache," + getNowTime());
            }
        }

        if (!directWriteToMq) {
            while (!(success = fileWorker.enqueue(byteData))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelMq) {
                    break;
                }
                ThreadUtils.sleep(5);
            }
        }

        if (success) {
            lastMessageId = messageId;
        } else {
            fileMissingCount.increment();
        }

        // clear必须在notify之前，否则notify之后，新的数据可能立马放入event中，后执行clear可能会把新的数据给clear掉
        event.clear();
        notifySeq(sequence);
    }

    private boolean isExclude(ILoggingEvent message) {
        if (isNull(message)) {
            return true;
        }
        return startsWithAny(
            message.getLoggerName(),
            "ch.qos.logback",
            "org.apache.pulsar",
            "org.apache.kafka"
        );
    }

    @Override
    public void close() {
        logMissingCount.close();
        fileMissingCount.close();
        isClosed = true;
        queue.shutdown();
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
    private void notifySeq(long currentSeq) {
        long nextNotifySeq = this.nextNotifySeq;
        if (0 == nextNotifySeq) {
            this.nextNotifySeq = currentSeq + 128;
            return;
        }
        if (currentSeq >= nextNotifySeq) {
            sequenceCallback.set(currentSeq);
            this.nextNotifySeq = currentSeq + 128;
        }
    }
}
