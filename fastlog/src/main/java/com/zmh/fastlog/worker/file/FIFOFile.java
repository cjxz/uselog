package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.zmh.fastlog.utils.Utils.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.isNull;

public class FIFOFile {
    private FilesManager filesManager;

    private ReadWriteFile writeFile;

    private ReadWriteFile readFile;

    private IndexFile indexFile;

    private long capacity;

    private int maxFileSize;

    public FIFOFile(@NonNull String queueName, @NonNull Path folder, long capacity, int maxFileSize) {
        this.filesManager = FilesManager.builder()
            .folder(folder)
            .prefix(queueName + '-')
            .suffix(".log")
            .build();

        this.capacity = capacity;
        this.indexFile = new IndexFile(folder, maxFileSize);
        this.maxFileSize = maxFileSize;
    }

    public void write(ByteData byteData) {
        if (isNull(writeFile) || !writeFile.write(byteData)) {
            Path path = filesManager.createNextFile();
            if (filesManager.getFileSize() > maxFileSize) {
                filesManager.remove(readFile.getPath());
                readFile = null;
            }

            writeFile = new ReadWriteFile(path, indexFile, 0, 0, capacity, filesManager.getIndex(path));
        }

        writeFile.write(byteData);
    }

    public ByteData read() {
        if (isNull(readFile)) {
            if (filesManager.getFileSize() == 1) {
                return writeFile.read();
            } else {
                Path path = filesManager.poll();
                readFile = new ReadWriteFile(path, indexFile, capacity, filesManager.getIndex(path));
            }
        }
        ByteData data = readFile.read();
        if (isNull(data)) {
            filesManager.remove(readFile.getPath());
            readFile = null;
        }
        return data;
    }
}

class ReadWriteFile implements Closeable {
    @Getter
    private Path path;
    private IndexFile indexFile;
    private FileChannel channel;
    private long readIndex;
    private long writeIndex;
    private long capacity;
    private int index;

    public ReadWriteFile(Path path, IndexFile indexFile, long capacity, int index) {
        this(path, indexFile, indexFile.readIndex(index), indexFile.writeIndex(index), capacity, index);
    }

    @SneakyThrows
    public ReadWriteFile(Path path, IndexFile indexFile, long readIndex, long writeIndex, long capacity, int index) {
        this.path = path;
        this.indexFile = indexFile;
        this.channel = FileChannel.open(path, WRITE, READ);
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;
        this.capacity = capacity;
        this.index = index;
    }

    private ByteBuffer lenBuffer = ByteBuffer.allocate(4);
    private ByteBuffer idBuffer = ByteBuffer.allocate(8);
    private ByteBuffer readByteBuffer = ByteBuffer.allocate(2048);


    public boolean write(ByteData byteData) {
        int len = byteData.getDataLength();
        if (writeIndex - readIndex + 96 + len * 8 > capacity) {
            return false;
        }

        lenBuffer.clear();
        lenBuffer.putInt(len);
        writeByteBuffer(lenBuffer, 32);

        idBuffer.clear();
        idBuffer.putLong(byteData.getId());
        writeByteBuffer(idBuffer, 64);

        ByteBuffer data = ByteBuffer.wrap(byteData.getData(), 0, len);
        writeByteBuffer(data, len * 8);
        return true;
    }

    public ByteData read() {
        if (!readByteBuffer(lenBuffer, 32)) {
            return null;
        }

        int len = lenBuffer.getInt();

        if (len <= 0) {
            debugLog("error");
            return null;
        }

        if (readByteBuffer.capacity() < len) {
            readByteBuffer = ByteBuffer.allocate(marginToBuffer(len));
        }

        readByteBuffer(idBuffer, 64);
        readByteBuffer(readByteBuffer, len * 8);

        return new ByteData(idBuffer.getLong(), readByteBuffer.array(), len);
    }

    @SneakyThrows
    private void writeByteBuffer(ByteBuffer buffer, int len) {
        buffer.rewind();

        long position = writeIndex % capacity;
        if (position + len < capacity) {
            this.channel.write(buffer, position);
        } else {
            int cut = (int)(capacity - position) >> 3;

            ByteBuffer onePart = ByteBuffer.wrap(buffer.array(), 0, cut);
            this.channel.write(onePart, position);

            ByteBuffer twoPart = ByteBuffer.wrap(buffer.array(), cut, len << 3 - cut);
            this.channel.write(twoPart, 0);
        }

        writeIndex += len;
        indexFile.write(this.index, len);

        buffer.clear();
    }

    @SneakyThrows
    private boolean readByteBuffer(ByteBuffer buffer, int len) {
        if (writeIndex - readIndex < len) {
            return false;
        }

        buffer.clear();

        long position = readIndex % capacity;
        if (index + len < capacity) {
            this.channel.read(buffer, position);
        } else {
            int cut = (int)(capacity - position) >> 3;


            this.channel.read(buffer, position);
            this.channel.read(buffer, 0);
        }

        readIndex += len;
        indexFile.read(this.index, len);

        buffer.flip();
        return true;
    }

    @Override
    public void close() {
        indexFile.close();
        safeClose(channel);
    }
}


class IndexFile implements Closeable {

    private MappedByteBuffer mbb;
    private int maxFileSize;
    private RandomAccessFile raf;

    @SneakyThrows
    public IndexFile(Path folder, int maxFileSize) {
        Path indexPath = folder.resolve("log.index");

        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        this.raf = new RandomAccessFile(indexPath.toFile(), "rwd");

        //把文件映射到内存
        this.mbb = this.raf.getChannel().map(MapMode.READ_WRITE, 0, maxFileSize * 64);
        this.maxFileSize = maxFileSize;
    }

    public void write(int fileIndex, int len) {
        int index = ((fileIndex % maxFileSize) << 6) + 32;
        mbb.putInt(index, mbb.getInt(index) + len);
    }

    public void read(int fileIndex, int len) {
        int index = (fileIndex % maxFileSize) << 6;
        mbb.putInt(index, mbb.getInt(index) + len);
    }

    public int readIndex(int fileIndex) {
        return mbb.getInt((fileIndex % maxFileSize) << 6);
    }

    public int writeIndex(int fileIndex) {
        return mbb.getInt(((fileIndex % maxFileSize) << 6) + 32);
    }

    @Override
    public void close() {
        safeClose(raf);
    }
}