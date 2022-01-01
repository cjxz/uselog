package com.zmh.fastlog.worker;

import ch.qos.logback.classic.pattern.CallerDataConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.utils.DateSequence;
import com.zmh.fastlog.utils.JsonByteBuilder;
import com.zmh.fastlog.utils.ThreadUtils;
import lombok.*;
import org.apache.commons.lang3.time.FastDateFormat;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.LongAdder;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleAtFixedRate;
import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getNowTime;
import static com.zmh.fastlog.worker.LogWorker.Consts.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.startsWithAny;

/**
 * @author zmh
 */
public class LogWorker implements Worker<Object>,
    EventHandler<LogWorker.LogEvent>,
    SequenceReportingEventHandler<LogWorker.LogEvent>,
    BatchStartAware {

    // 日志有两个可能方向, 一个往pulsar, 一个写本地文件缓存
    // 以下两个是高水位阈值, 日志堆积超过这个阈值后应该丢弃之前的日志,
    // 这种策略同时能达到两个目地:
    //   1. 缓冲区空间足够的时候尽可能的等后续渠道消费日志, 空间紧张时预先丢弃不重要的日志
    //   2. 日志和同步SEQ都是通过消息发送, 但消息优先级应该更高, 通过丢弃之前的同步SEQ消息使得消息能更快被处理
    @Getter
    private final int highWaterLevelPulsar;
    @Getter
    private final int highWaterLevelFile;
    // 日志缓冲区
    private final Disruptor<LogEvent> queue;
    private final RingBuffer<LogEvent> ringBuffer;
    private volatile boolean isClosed = false;

    // 统计丢弃的日志数
    private long totalMissingCount = 0;
    private final LongAdder missingCount1 = new LongAdder();
    private final LongAdder missingCount2 = new LongAdder();
    private final ScheduledFuture<?> missingSchedule = scheduleAtFixedRate(this::reportMissCount, 1, 5, SECONDS);


    private Map<String, String> additionalFields;

    // 消息去向, 二选1
    // 初始时先通过file,file缓冲区为空的切到mq
    // mq堵塞的时候切到file缓存
    private boolean directWriteToPulsar = false;
    private final Worker<Object> pulsarWorker;
    private final Worker<LogEvent> fileQueueWorker;


    public LogWorker(Worker<Object> pulsarWorker, Worker<LogEvent> fileQueueWorker, int batchSize, Map<String, String> additionalFields) {
        this.pulsarWorker = pulsarWorker;
        this.fileQueueWorker = fileQueueWorker;
        this.additionalFields = additionalFields;
        // 缓冲区设置
        // 初始的缓冲池, 避免短期内日志突然增多造成日志来不及处理而丢失
        // 本实例是日志的入口, 尽量通过缓冲区把各个线程的日志的平缓的收集过来
        int bufferSize = batchSize << 2;
        this.highWaterLevelFile = (int) (bufferSize * 0.75);
        this.highWaterLevelPulsar = bufferSize >> 1;

        queue = new Disruptor<>(
            LogEvent::new,
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
                missingCount1.increment();
                return false;
            } else {
                return true;
            }
        } else if (message instanceof LastSeq) {
            long lastSeq = ((LastSeq) message).getSeq();
            // 本地文件缓冲区已经发完了, 后续日志切换到mq
            if (lastSeq == lastMessageId && !directWriteToPulsar) {
                directWriteToPulsar = true;
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
    //private long lastMessageId = currentTimeMillis() << 12;
    private long lastMessageId = 0;

    public void onEvent(LogEvent event, long sequence, boolean endOfBatch) {
        long messageId = lastMessageId + 1;
        event.setId(messageId);

        boolean success = false;

        if (directWriteToPulsar) {
            while (!(success = pulsarWorker.sendMessage(event))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelPulsar) {
                    break;
                }
                ThreadUtils.sleep(1);
            }
            // 写入失败, 切换到本地文件缓冲区
            if (!success && directWriteToPulsar) {
                directWriteToPulsar = false;
                debugLog("pulsar阻塞,切换到file cache," + getNowTime());
            }
        }

        if (!directWriteToPulsar) {
            while (!(success = fileQueueWorker.sendMessage(event))) {
                if (isClosed) {
                    break;
                }
                if (ringBuffer.getCursor() - sequence >= highWaterLevelPulsar) {
                    break;
                }
                ThreadUtils.sleep(1);
            }
        }

        if (success) {
            lastMessageId = messageId;
        } else {
            missingCount2.increment();
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
            jsonByteBuilder.key(DATA_MDC).beginObject();
            mdc.forEach((k, v) -> jsonByteBuilder.key(k).value(v));
            jsonByteBuilder.endObject();
        }

        if (nonNull(additionalFields)) {
            additionalFields.forEach((k, v) -> jsonByteBuilder.key(k).value(v));
        }

        jsonByteBuilder.endObject();
    }

    private void reportMissCount() {
        long sum1 = missingCount1.sumThenReset();
        if (sum1 > 0) {
            totalMissingCount += sum1;
            debugLog("log mission count1:" + sum1 + ", total:" + totalMissingCount);
        }

        long sum2 = missingCount2.sumThenReset();
        if (sum2 > 0) {
            totalMissingCount += sum2;
            debugLog("log mission count2:" + sum2 + ", total:" + totalMissingCount);
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
        static final String DATA_MSG = "msg"; //todo
        static final String DATA_MESSAGE = "message"; //todo
        static final String DATA_LOGGER = "logger";
        static final String DATA_THREAD = "thread";
        static final String DATA_LEVEL = "level";
        static final String DATA_MARKER = "marker"; //todo
        static final String DATA_CALLER = "caller"; //todo
        static final String DATA_SEQ = "seq";
        static final String DATA_THROWABLE = "throwable";
        static final String DATA_TIME_MILLSECOND = "ts"; //todo
        static final String DATA_IP = "ip"; //todo
        static final String DATA_MDC = "mdc";
        static final String DATA_TIMESTAMP = "@timestamp";
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @ToString(callSuper = true)
    public static class LogEvent extends ByteMessage {
        private JsonByteBuilder byteBuilder = JsonByteBuilder.create(2 * 1024);
        private WeakReference<JsonByteBuilder> cache = new WeakReference<>(byteBuilder);

        public void clear() {
            if (byteBuilder != null) {
                if (byteBuilder.capacity() > 2048) {
                    cache = new WeakReference<>(byteBuilder);
                    byteBuilder = null;
                } else {
                    byteBuilder.clear();
                }
            }
        }

        public JsonByteBuilder getByteBuilder() {
            if (null == byteBuilder) {
                byteBuilder = cache.get();
                if (null == byteBuilder) {
                    byteBuilder = JsonByteBuilder.create(2048);
                }
            }
            return byteBuilder;
        }

        @Override
        public void apply(ByteEvent event) {
            event.clear();

            ByteBuffer buff = byteBuilder.toByteBuffer(event.getBuffer());
            event.setId(this.getId());
            event.setBuffer(buff);
            event.setBufferLen(buff.position());
        }
    }


}
