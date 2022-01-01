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
import com.zmh.fastlog.worker.backend.LogFiles;
import io.appulse.utils.Bytes;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
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
        return ringBuffer.tryPublishEvent((e, s) -> {
            message.apply(e.getByteEvent());
        });
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
            if (isClose || !pulsarWorker.sendMessage(message)) {
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

    private final BytesCacheQueue tail;

    private final LogFiles logFiles;

    private final BytesCacheQueue head;

    private final Bytes bytesClone;

    FIFOFileQueue(int cacheSize) {
        logFiles = LogFiles.builder()
            .queueName("queue")
            .restoreFromDisk(true)
            .config(WalFilesConfig.builder()
                .folder("logs/cache")
                .maxCount(100)
                .build())
            .build();

        //Files.createDirectories(backend.); todo zmh cannot not-exit

        tail = new BytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
        bytesClone = Bytes.allocate(cacheSize + Integer.BYTES);
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
            logFiles.pollTo(bytesClone);
            head.readFrom(bytesClone);
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
        tail.writeTo(bytesClone);
        logFiles.write(bytesClone);
        tail.reset();
    }

    @Override
    public void close() {
        flush();
        logFiles.close();
    }

}


class BytesCacheQueue {
    @Getter
    private final Bytes bytes;

    private AtomicInteger size;

    public BytesCacheQueue(int size) {
        this.bytes = Bytes.allocate(size);
        this.size = new AtomicInteger();
    }

    public boolean put(ByteEvent event) {
        ByteBuffer bb = event.getBuffer();
        val writerIndex = this.bytes.writerIndex();

        if (writerIndex + Long.BYTES + Integer.BYTES + bb.limit() + Integer.BYTES > bytes.capacity()) {
            this.bytes.write4B(-1);
            return false;
        }

        this.bytes.write4B(0); // write fake length
        this.bytes.write8B(event.getId());
        this.bytes.writeNB(bb.array());

        this.bytes.set4B(writerIndex, this.bytes.writerIndex() - writerIndex - Integer.BYTES - Long.BYTES); // write real length
        this.size.incrementAndGet();
        return true;
    }

    private byte[] readBuffer = new byte[5120];

    public DataByteMessage get() {
        if (bytes.readableBytes() == 0) {
            return null;
        }
        int readCount = this.bytes.readInt();
        if (readCount > 0) {
            if (readCount > readBuffer.length) {
                readBuffer = new byte[marginToBuffer(readCount)];
            }
            long id = this.bytes.readLong();
            this.bytes.readBytes(readBuffer, 0, readCount);
            this.size.decrementAndGet();

            return new DataByteMessage(id, readBuffer, readCount);
        } else if (readCount == -1) {
            reset();
        }
        return null;
    }

    public void reset() {
        this.bytes.reset();
        this.size = new AtomicInteger();
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    public int getSize() {
        return size.get();
    }

    public void copyTo(BytesCacheQueue queue) {
        queue.reset();

        queue.bytes.writeNB(this.bytes.array(), this.bytes.readerIndex(), this.bytes.readableBytes());
        queue.size = new AtomicInteger(getSize());
    }

    public void readFrom(Bytes clone) {
        reset();

        int size = clone.readInt();
        this.size = new AtomicInteger(size);
        this.bytes.writeNB(clone.array(), clone.readerIndex(), clone.readableBytes());
    }

    public void writeTo(Bytes clone) {
        clone.reset();

        clone.write4B(getSize());
        clone.writeNB(this.bytes.array(), this.bytes.readerIndex(), this.bytes.readableBytes());
    }


}

