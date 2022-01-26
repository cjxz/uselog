package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import io.appulse.utils.Bytes;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.Future;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.marginToBuffer;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class FIFOQueue implements AutoCloseable {

    /**
     * 队列尾巴 是 写缓冲区
     * 使用双内存队列的原因是，当其中一个队列满的时候需要flush到磁盘，这段时间比较长，如果后续日志发送又快的话，会造成日志丢失
     * 所以将flush的操作异步，异步期间使用另外一个内存队列存放日志数据
     */
    private final TwoBytesCacheQueue tail;

    private final LogFilesManager logFiles;

    /**
     * 队列头 是 读缓冲区
     * 队列头的数据来源有两种场景：
     * 1、如果日志还没有多到需要放入磁盘的话，会将写缓冲区的日志数据直接复制队列头，这样写缓冲区可以继续写，读缓冲区可以从内存中读取，
     *    注意：这里的写缓冲区的数据不一定是满的，因为当日志的写入和读取速度相当的时候，日志可以直接从写缓冲区中获取，而不一定是非得从读缓冲区中获取
     * 2、如果日志多到已经写入磁盘，那最早的日志数据一定在磁盘文件，此时需要从磁盘中读取文件写入该读缓冲区，供后续读取日志使用
     */
    private final BytesCacheQueue head;

    @SneakyThrows
    FIFOQueue(String folder, int cacheSize, int maxFileCount) {
        logFiles = LogFilesManager.builder()
            .folder(folder)
            .cacheSize(cacheSize)
            .maxIndex(8)
            .maxFileNum(maxFileCount)
            .build();

        tail = new TwoBytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
    }

    public void put(ByteData byteData) {
        if (tail.put(byteData)) {
            return;
        }

        if (head.isEmpty() && logFiles.isEmpty()) {
            tail.copyTo(head);
            tail.reset();
        } else {
            flush();
        }

        tail.put(byteData);
    }

    private ByteData current;

    public ByteData get() {
        if (isNull(current)) {
            next();
        }
        return current;
    }

    public void next() {
        current = head.get();
        if (nonNull(current)) {
            return;
        }

        if (!logFiles.isEmpty()) {
            logFiles.pollTo(head.getBytes());
            current = head.get();
            return;
        }

        current = tail.get();
    }

    // for test
    public int getFileNum() {
        return logFiles.getFileSize();
    }

    public int getTotalFile() {
        return logFiles.getTotalFile();
    }

    public void flush() {
        if (tail.isEmpty()) {
            return;
        }

        tail.waitFutureDone();
        Future<?> future = logFiles.write(tail.getBytes());
        tail.flush(future);
    }

    @Override
    public void close() {
        flush();
        logFiles.close();
    }

}

class TwoBytesCacheQueue {

    private BytesCacheQueueFlush used;
    private BytesCacheQueueFlush other;

    public TwoBytesCacheQueue(int size) {
        used = new BytesCacheQueueFlush(size);
        other = new BytesCacheQueueFlush(size);
    }

    public void flush(Future<?> future) {
        this.used.flush(future);
        switchQueue();
    }

    public void waitFutureDone() {
        this.other.waitFutureDone();
    }

    private void switchQueue() {
        other.getQueue().reset();
        BytesCacheQueueFlush temp = used;
        used = other;
        other = temp;
    }

    public boolean put(ByteData byteData) {
        return this.used.getQueue().put(byteData);
    }

    public ByteData get() {
        return this.used.getQueue().get();
    }

    public void reset() {
        this.used.getQueue().reset();
    }

    public boolean isEmpty() {
        return this.used.getQueue().isEmpty();
    }

    public void copyTo(BytesCacheQueue queue) {
        this.used.getQueue().copyTo(queue);
    }

    public Bytes getBytes() {
        return this.used.getQueue().getBytes();
    }
}

class BytesCacheQueueFlush {
    @Getter
    private final BytesCacheQueue queue;
    private Future<?> future;

    public BytesCacheQueueFlush(int size) {
        this.queue = new BytesCacheQueue(size);
    }

    public void flush(Future<?> future) {
        this.future = future;
    }

    @SneakyThrows
    public void waitFutureDone() {
        if (isNull(future) || future.isDone()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        future.get();
        stopWatch.stop();
        debugLog("wait future:" + stopWatch.formatTime());
    }
}

class BytesCacheQueue {
    @Getter
    private final Bytes bytes;

    public BytesCacheQueue(int size) {
        this.bytes = Bytes.allocate(size);
    }

    public boolean put(ByteData byteData) {
        int dataLength = byteData.getDataLength();
        if (this.bytes.writerIndex() + Long.BYTES + Integer.BYTES + dataLength > bytes.capacity()) {
            return false;
        }

        this.bytes.write4B(dataLength); //日志的长度 单位：字节
        this.bytes.write8B(byteData.getId());
        this.bytes.writeNB(byteData.getData(), 0, dataLength);
        return true;
    }

    private byte[] readBuffer = new byte[5120];

    public ByteData get() {
        if (bytes.readableBytes() == 0) {
            this.bytes.reset();
            return null;
        }
        int readCount = this.bytes.readInt();
        if (readCount > 0) {
            if (readCount > readBuffer.length) {
                readBuffer = new byte[marginToBuffer(readCount)];
            }
            long id = this.bytes.readLong();
            this.bytes.readBytes(readBuffer, 0, readCount);

            return new ByteData(id, readBuffer, readCount);
        } else {
            debugLog("fastlog BytesCacheQueue readCount error " + readCount);
            this.bytes.reset();
            return null;
        }
    }

    public void reset() {
        this.bytes.reset();
    }

    public boolean isEmpty() {
        return bytes.readableBytes() == 0;
    }

    public void copyTo(BytesCacheQueue queue) {
        queue.reset();

        queue.bytes.writeNB(this.bytes.array(), this.bytes.readerIndex(), this.bytes.readableBytes());
    }

}
