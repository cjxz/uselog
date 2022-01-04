package com.zmh.fastlog.worker;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteTimeoutBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zmh.fastlog.config.WalFilesConfig;
import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.worker.LogWorker.LogEvent;
import com.zmh.fastlog.worker.file.BytesCacheQueue;
import com.zmh.fastlog.worker.file.LogFiles;
import com.zmh.fastlog.worker.file.TwoBytesCacheQueue;
import lombok.Setter;
import lombok.SneakyThrows;

import java.lang.ref.SoftReference;
import java.nio.file.Files;

import static com.zmh.fastlog.utils.ThreadUtils.namedDaemonThreadFactory;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileWorker implements Worker<LogEvent>, EventHandler<FileEvent>, TimeoutHandler {

    private final Worker<Object> pulsarWorker;
    private final Disruptor<FileEvent> queue;
    private final RingBuffer<FileEvent> ringBuffer;
    private final FIFOFileQueue fifo;
    private static final int BUFFER_SIZE = 2048;
    private static final int HIGH_WATER_LEVEL_FILE = BUFFER_SIZE >> 1;

    private volatile boolean isClose;

    public FileWorker(Worker<Object> pulsarWorker) {
        fifo = new FIFOFileQueue(64 * 1024 * 1024);

        this.pulsarWorker = pulsarWorker;
        queue = new Disruptor<>(
            FileEvent::new,
            BUFFER_SIZE,
            namedDaemonThreadFactory("log-filequeue-worker"),
            ProducerType.SINGLE,
            new LiteTimeoutBlockingWaitStrategy(100, MILLISECONDS)
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();
    }

    @Override
    public boolean sendMessage(LogEvent message) {
        return ringBuffer.tryPublishEvent((e, s) -> message.apply(e.getByteEvent()));
    }

    @Override
    public void onEvent(FileEvent event, long sequence, boolean endOfBatch) {
        fifo.put(event.getEvent());
        event.clear();

        if (endOfBatch) {
            onTimeout(sequence);
        }
    }

    @Override
    public void onTimeout(long sequence) {
        ByteMessage message;
        while (ringBuffer.getCursor() - sequence <= HIGH_WATER_LEVEL_FILE && nonNull(message = fifo.get())) {
            if (isClose || !pulsarWorker.sendMessage(message)) {  //todo zmh 这里得比后续Kafka后加载，避免大量日志发出去报错。
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

class FileEvent {
    @Setter
    private ByteEvent event;

    private SoftReference<ByteEvent> byteEventRef = new SoftReference<>(new ByteEvent());

    private ByteEvent refKeep;

    ByteEvent getByteEvent() {
        if (nonNull(refKeep)) {
            return refKeep;
        }
        ByteEvent event = this.byteEventRef.get();
        if (isNull(event)) {
            event = new ByteEvent();
            byteEventRef = new SoftReference<>(event);
        }
        refKeep = event;
        return event;
    }

    void clear() {
        if (nonNull(refKeep)) {
            refKeep.clear();
            refKeep = null;
        }
        event = null;
    }

    public ByteEvent getEvent() {
        if (nonNull(event)) {
            return event;
        }
        return getByteEvent();
    }
}

class FIFOFileQueue implements AutoCloseable {

    private final TwoBytesCacheQueue tail;

    private final LogFiles logFiles;

    private final BytesCacheQueue head;


    @SneakyThrows
    FIFOFileQueue(int cacheSize) {
        WalFilesConfig config = WalFilesConfig.builder()
            .folder("logs/cache")
            .maxCount(100)
            .build();

        if (!Files.exists(config.getFolder())) {
            Files.createDirectories(config.getFolder());
        }

        logFiles = LogFiles.builder()
            .queueName("queue")
            .config(config)
            .cacheSize(cacheSize)
            .build();

        tail = new TwoBytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
    }

    public void put(ByteEvent byteBuffer) {
        if (tail.put(byteBuffer)) {
            return;
        }

        if (head.isEmpty() && fileSize() == 0) {
            tail.copyTo(head);
            tail.reset();
        } else {
            flush();
            tail.switchHead();
        }

        tail.put(byteBuffer);
    }

    private ByteMessage message;

    public ByteMessage get() {
        if (nonNull(message)) {
            return message;
        }

        message = head.get();
        if (nonNull(message)) {
            return message;
        }

        if (fileSize() > 0) {
            logFiles.pollTo(head.getBytes());
            message = head.get();
            return message;
        }

        message = tail.get();
        return message;
    }

    public void next() {
        message = null;
    }

    // for test
    int fileSize() {
        return logFiles.getFileSize();
    }

    public void flush() {
        if (tail.isEmpty()) {
            return;
        }
        logFiles.write(tail.getUsed());
    }

    @Override
    public void close() {
        flush();
        logFiles.close();
    }

}


