package com.zmh.fastlog.worker.log;

import ch.qos.logback.classic.pattern.CallerDataConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.model.event.LogDisruptorEvent;
import com.zmh.fastlog.utils.DateSequence;
import com.zmh.fastlog.utils.JsonByteBuilder;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.Worker;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import com.zmh.fastlog.model.message.LastConfirmedSeq;
import lombok.Getter;
import lombok.val;
import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.LongAdder;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleAtFixedRate;
import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getNowTime;
import static com.zmh.fastlog.worker.log.LogWorker.Consts.*;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.startsWithAny;

/**
 * @author zmh
 */
public class LogWorker implements Worker<Object>,
    EventHandler<LogDisruptorEvent>,
    SequenceReportingEventHandler<LogDisruptorEvent>,
    BatchStartAware {

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
    private final Disruptor<LogDisruptorEvent> queue;
    private final RingBuffer<LogDisruptorEvent> ringBuffer;
    private volatile boolean isClosed = false;

    // 统计丢弃的日志数
    private long totalMissingCount = 0;
    private final LongAdder logMissingCount = new LongAdder();
    private final LongAdder fileMissingCount = new LongAdder();
    private final ScheduledFuture<?> missingSchedule = scheduleAtFixedRate(this::reportMissCount, 1, 5, SECONDS);

    // 消息去向, 二选1
    // 初始时先通过file,file缓冲区为空的切到mq
    // mq堵塞的时候切到file缓存
    private boolean directWriteToMq = false;
    private final Worker<AbstractMqMessage> mqWorker;
    private final Worker<LogDisruptorEvent> fileWorker;
    private final int logMaxSize; //todo zmh
    private final String largeLogHandlerType;

    public LogWorker(Worker<AbstractMqMessage> mqWorker, Worker<LogDisruptorEvent> fileWorker, int batchSize, int logMaxSize, String largeLogHandlerType) {
        this.logMaxSize = logMaxSize;
        this.largeLogHandlerType = largeLogHandlerType;

        this.mqWorker = mqWorker;
        this.fileWorker = fileWorker;
        // 缓冲区设置
        // 初始的缓冲池, 避免短期内日志突然增多造成日志来不及处理而丢失
        // 本实例是日志的入口, 尽量通过缓冲区把各个线程的日志的平缓的收集过来
        int bufferSize = batchSize << 2;
        this.highWaterLevelFile = (int) (bufferSize * 0.75);
        this.highWaterLevelMq = bufferSize >> 1;

        queue = new Disruptor<>(
            LogDisruptorEvent::new,
            bufferSize,
            namedDaemonThreadFactory("log-log-worker"),
            ProducerType.MULTI, // 注意此处为多生产者
            new LiteBlockingWaitStrategy()
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();
    }

    /**
     * log ring buffer 生产者
     *
     * @param message 入参有两种类型，1、正常日志 ILoggingEvent  2、mq发过来的已消费序号 LastSeq
     * @return true 日志发送成功 false 日志发送失败
     */
    @SuppressWarnings("CodeBlock2Expr")
    @Override
    public boolean sendMessage(Object message) {
        if (message instanceof ILoggingEvent) {
            val msg = (ILoggingEvent) message;
            if (isExclude(msg)) {
                return true;
            }
            if (!ringBuffer.tryPublishEvent((event, sequence) -> {
                convertToByteMessage(msg, event.getByteBuilder());
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

    private boolean isExclude(ILoggingEvent message) {
        if (isNull(message)) {
            return true;
        }
        return startsWithAny(
            message.getLoggerName(),
            "ch.qos.logback",
            "org.apache.pulsar",
            "org.apache.kafka",
            "org.rocksdb"
        );
    }

    @Override
    public void close() {
        missingSchedule.cancel(true);
        reportMissCount();
        isClosed = true;
        queue.shutdown();
    }

    // 日志id, 发送成功一条加1,
    // 初始值 [ currentMillSeconds, 0 ]
    //       [  41bit,    12bit       ]
    private long lastMessageId = currentTimeMillis() << 12;

    public void onEvent(LogDisruptorEvent event, long sequence, boolean endOfBatch) {
        long messageId = lastMessageId + 1;
        event.setId(messageId);

        boolean success = false;

        if (directWriteToMq) {
            while (!(success = mqWorker.sendMessage(event))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelMq) {
                    break;
                }
                ThreadUtils.sleep(1);
            }
            // 写入失败, 切换到本地文件缓冲区
            if (!success && directWriteToMq) {
                directWriteToMq = false;
                debugLog("mq阻塞或者没准备好,切换到file cache," + getNowTime());
            }
        }

        if (!directWriteToMq) {
            while (!(success = fileWorker.sendMessage(event))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelMq) {
                    break;
                }
                ThreadUtils.sleep(1);
            }
        }

        if (success) {
            lastMessageId = messageId;
        } else {
            fileMissingCount.increment();
        }

        notifySeq(sequence);
        event.clear();
    }

    private final static DateSequence dataSeq = new DateSequence();
    private final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    void convertToByteMessage(ILoggingEvent log, JsonByteBuilder jsonByteBuilder) {
        jsonByteBuilder.clear()
            .beginObject()
            .key(DATA_SEQ).value(dataSeq.next())
            .key(DATA_MESSAGE).value(log.getFormattedMessage())
            .key(DATA_LOGGER).value(log.getLoggerName())
            .key(DATA_THREAD).value(log.getThreadName())
            .key(DATA_LEVEL).value(log.getLevel().levelStr);

        long timeStamp = log.getTimeStamp();
        jsonByteBuilder
            .key(DATA_TIME_MILLSECOND).value(timeStamp)
            .key(DATA_TIMESTAMP).value(dateFormat.format(timeStamp));

        if (nonNull(log.getMarker())) {
            jsonByteBuilder
                .key(DATA_MARKER)
                .value(log.getMarker().toString());
        }
        if (log.hasCallerData()) {
            jsonByteBuilder
                .key(DATA_CALLER)
                .value(new CallerDataConverter().convert(log));
        }
        if (nonNull(log.getThrowableProxy())) {
            jsonByteBuilder
                .key(DATA_THROWABLE)
                .value(ThrowableProxyUtil.asString(log.getThrowableProxy()));
        }
        Map<String, String> mdc = log.getMDCPropertyMap();
        if (mdc.size() > 0) {
            mdc.forEach((k, v) -> jsonByteBuilder.key(k).value(v));
        }

        jsonByteBuilder.endObject();
    }

    private void reportMissCount() {
        long sum1 = logMissingCount.sumThenReset();
        if (sum1 > 0) {
            totalMissingCount += sum1;
            debugLog("log mission count:" + sum1 + ", total:" + totalMissingCount);
        }

        long sum2 = fileMissingCount.sumThenReset();
        if (sum2 > 0) {
            totalMissingCount += sum2;
            debugLog("file mission count:" + sum2 + ", total:" + totalMissingCount);
        }
    }

    private Sequence sequenceCallback;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }

    private long nextNotifySeq = 0;

    @Override
    public void onBatchStart(long batchSize) {
        nextNotifySeq = 0;
    }

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

    @SuppressWarnings("unused")
    static class Consts {
        static final String DATA_MESSAGE = "message";
        static final String DATA_LOGGER = "logger";
        static final String DATA_THREAD = "thread";
        static final String DATA_LEVEL = "level";
        static final String DATA_MARKER = "marker"; //todo
        static final String DATA_CALLER = "caller"; //todo
        static final String DATA_SEQ = "seq";
        static final String DATA_THROWABLE = "throwable";
        static final String DATA_TIME_MILLSECOND = "ts";
        static final String DATA_TIMESTAMP = "@timestamp";
    }
}
