package com.zmh.fastlog.worker.file.fifo;

import lombok.SneakyThrows;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.zmh.fastlog.utils.Utils.safeClose;

public class IndexFile implements Closeable {

    private MappedByteBuffer mbb;
    private RandomAccessFile raf;

    @SneakyThrows
    public IndexFile(Path folder) {
        Path indexPath = folder.resolve("log.index");

        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }

        this.raf = new RandomAccessFile(indexPath.toFile(), "rwd");

        //把文件映射到内存
        this.mbb = this.raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 32);
    }

    public void write(int fileIndex, long len) {
        int position = position(fileIndex) + 8;
        mbb.putLong(position, mbb.getLong(position) + len);
    }

    public void read(int fileIndex, long len) {
        int position = position(fileIndex);
        mbb.putLong(position, mbb.getLong(position) + len);
    }

    public long readIndex(int fileIndex) {
        return mbb.getLong(position(fileIndex));
    }

    public long writeIndex(int fileIndex) {
        return mbb.getLong(position(fileIndex) + 8);
    }

    public void reset(int fileIndex) {
        int position = position(fileIndex);
        mbb.putLong(position, 0);
        mbb.putLong(position + 8, 0);
    }

    public void reset(int fileIndex, long readIndex, long writeIndex) {
        int position = position(fileIndex);
        mbb.putLong(position, readIndex);
        mbb.putLong(position + 8, writeIndex);
    }

    private int position(int fileIndex) {
        return (fileIndex & 1) << 4;
    }

    @Override
    public void close() {
        safeClose(raf);
    }
}
