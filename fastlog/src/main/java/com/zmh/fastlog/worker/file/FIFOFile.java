package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.marginToBuffer;
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

class ReadWriteFile {
    @Getter
    private Path path;
    private IndexFile indexFile;
    private FileChannel channel;
    private long readIndex;
    private long writeIndex;
    private long capacity;
    private int index;

    public ReadWriteFile(Path path, IndexFile indexFile, long capacity, int index) {
        new ReadWriteFile(path, indexFile, indexFile.readIndex(index), indexFile.writeIndex(index), capacity, index);
    }

    @SneakyThrows
    public ReadWriteFile(Path path, IndexFile indexFile, long readIndex, long writeIndex, long capacity, int index) {
        this.path = path;
        this.indexFile = indexFile;
        this.channel = FileChannel.open(path);
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;
        this.capacity = capacity;
        this.index = index;
    }

    private ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    private ByteBuffer idBuffer = ByteBuffer.allocate(8);

    public boolean write(ByteData byteData) {
        int len = byteData.getDataLength();
        if (writeIndex - readIndex + 96 + len * 8 > capacity) {
            return false;
        }

        lenBuffer.putInt(len);
        writeByteBuffer(lenBuffer, 32);

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
        this.channel.write(buffer, writeIndex % capacity);
        writeIndex += len;
        indexFile.write(index, len);
    }

    private ByteBuffer readByteBuffer = ByteBuffer.allocate(2048);

    @SneakyThrows
    private boolean readByteBuffer(ByteBuffer buffer, int len) {
        if (writeIndex - readIndex < len) {
            return false;
        }

        this.channel.read(buffer, readIndex % capacity);
        readIndex += len;
        indexFile.read(index, len);
        return true;
    }

}


class IndexFile {

    private MappedByteBuffer mbb;
    private int maxFileSize;

    @SneakyThrows
    public IndexFile(Path folder, int maxFileSize) {
        Path indexPath = folder.resolve("log.index");

        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        RandomAccessFile raf = new RandomAccessFile(indexPath.toFile(), "rwd");
        FileChannel channel = raf.getChannel();

        //把文件映射到内存
        this.mbb = channel.map(MapMode.READ_WRITE, 0, maxFileSize * 64);
        this.maxFileSize = maxFileSize;
    }

    public void write(int fileIndex, int len) {
        int index = ((fileIndex % maxFileSize) << 8) + 32;
        mbb.putInt(index, mbb.getInt(index) + len);
    }

    public void read(int fileIndex, int len) {
        int index = (fileIndex % maxFileSize) << 8;
        mbb.putInt(index, mbb.getInt(index) + len);
    }

    public int readIndex(int fileIndex) {
        return mbb.getInt((fileIndex % maxFileSize) << 8);
    }

    public int writeIndex(int fileIndex) {
        return mbb.getInt(((fileIndex % maxFileSize) << 8) + 32);
    }

}