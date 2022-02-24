package com.zmh.fastlog.worker.file.fifo;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.safeClose;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Getter
public class ReadWriteFile implements Closeable {
    private Path path;
    private IndexFile indexFile;
    private FileChannel channel;
    private long readIndex;
    private long writeIndex;
    private long capacity;
    private int fileIndex;

    @SuppressWarnings("WeakerAccess")
    @SneakyThrows
    public ReadWriteFile(Path path, IndexFile indexFile, long readIndex, long writeIndex, long capacity, int fileIndex) {
        this.path = path;
        this.indexFile = indexFile;
        this.channel = FileChannel.open(path, WRITE, READ);
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;
        this.capacity = capacity;
        this.fileIndex = fileIndex;

        this.endBuffer = ByteBuffer.allocate(4);
        this.endBuffer.putInt(-1);
    }

    private ByteBuffer endBuffer;
    private ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    public boolean write(ByteBuffer buffer) {
        int len = buffer.remaining();

        boolean isFull = writeIndex - readIndex + len + 4 > capacity;

        long position = writeIndex & (capacity - 1);
        if (position + len + 8 >= capacity) {
            resetWritePosition();
            return !isFull && write(buffer);
        }

        if (isFull) {
            return false;
        }

        lenBuffer.clear();
        lenBuffer.putInt(len);
        writeByteBuffer(lenBuffer);

        writeByteBuffer(buffer);
        return true;
    }

    public boolean pollTo(ByteBuffer buffer) {
        if (!readByteBuffer(lenBuffer, 4)) {
            return false;
        }

        int len = lenBuffer.getInt();
        if (len == -1) {
            resetReadPosition();
            return pollTo(buffer);
        }

        if (len <= 0 || len > 1 << 30) { // > 1GB 异常
            debugLog("read error"); // todo zmh throw exception
            return false;
        }

        return readByteBuffer(buffer, len);
    }

    public boolean isEmpty() {
        return readIndex == writeIndex;
    }

    @SneakyThrows
    private void writeByteBuffer(ByteBuffer buffer) {
        buffer.rewind();
        int len = buffer.limit();

        long position = writeIndex & (capacity - 1);
        this.channel.write(buffer, position);

        writeIndex += len;
        indexFile.write(this.fileIndex, len);

        buffer.clear();
    }

    @SneakyThrows
    private boolean readByteBuffer(ByteBuffer buffer, int len) {
        if (writeIndex - readIndex < len) {
            return false;
        }

        buffer.clear();
        buffer.limit(len);

        long position = readIndex & (capacity - 1);
        this.channel.read(buffer, position);

        readIndex += len;
        indexFile.read(this.fileIndex, len);

        buffer.flip();
        return true;
    }

    private void resetReadPosition() {
        long position = readIndex & (capacity - 1);
        long len = capacity - position;

        readIndex += len;
        indexFile.read(this.fileIndex, len);
    }

    @SneakyThrows
    private void resetWritePosition() {
        long position = writeIndex & (capacity - 1);
        long len = capacity - position;

        endBuffer.rewind();
        channel.write(endBuffer, position);

        writeIndex += len;
        indexFile.write(fileIndex, len);
    }

    @Override
    public void close() {
        indexFile.close();
        safeClose(channel);
    }
}
