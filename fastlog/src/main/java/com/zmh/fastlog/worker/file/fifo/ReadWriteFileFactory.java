package com.zmh.fastlog.worker.file.fifo;

import java.nio.file.Path;

public class ReadWriteFileFactory {
    public static ReadWriteFile createWriteFile(Path path, IndexFile indexFile, long capacity) {
        indexFile.reset(0);
        return createWriteFile(path, indexFile, 0, 0, capacity);
    }

    public static ReadWriteFile createWriteFile(Path path, IndexFile indexFile, long readIndex, long writeIndex, long capacity) {
        return new ReadWriteFile(path, indexFile, readIndex, writeIndex, capacity, 0);
    }

    public static ReadWriteFile createReadFile(Path path, IndexFile indexFile, long capacity) {
        return createReadFile(path, indexFile, 0, capacity, capacity);
    }

    public static ReadWriteFile createReadFile(Path path, IndexFile indexFile, long readIndex, long writeIndex, long capacity) {
        indexFile.reset(1, readIndex, writeIndex);
        return new ReadWriteFile(path, indexFile, readIndex, writeIndex, capacity, 1);
    }
}
