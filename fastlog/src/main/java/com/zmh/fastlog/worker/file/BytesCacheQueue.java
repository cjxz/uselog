package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.worker.DataByteMessage;
import io.appulse.utils.Bytes;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang3.time.StopWatch;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.util.Objects.isNull;

public class BytesCacheQueue {
    @Getter
    private final Bytes bytes;

    private Future<?> future;

    public BytesCacheQueue(int size) {
        this.bytes = Bytes.allocate(size);
    }

    public boolean put(ByteEvent event) {
        ByteBuffer bb = event.getBuffer();
        val writerIndex = this.bytes.writerIndex();

        if (writerIndex + Long.BYTES + Integer.BYTES + bb.limit() > bytes.capacity()) {
            return false;
        }

        this.bytes.write4B(0); // write fake length
        this.bytes.write8B(event.getId());
        this.bytes.writeNB(bb.array());

        this.bytes.set4B(writerIndex, this.bytes.writerIndex() - writerIndex - Integer.BYTES - Long.BYTES); // write real length
        return true;
    }

    private byte[] readBuffer = new byte[5120];

    public DataByteMessage get() {
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

            return new DataByteMessage(id, readBuffer, readCount);
        } else {
            return null; //todo zmh throw exception
        }
    }

    public void asyncToDisk(Future<?> future) {
        this.future = future;
    }

    @SneakyThrows
    public void checkPut() {
        if (isNull(future) || future.isDone()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        future.get();
        stopWatch.stop();
        System.out.println("wait future:" + stopWatch.formatTime());
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
