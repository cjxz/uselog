package com.zmh.fastlog.worker.file.fifo;

import com.zmh.fastlog.model.message.ByteData;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.zmh.fastlog.utils.Utils.safeClose;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createReadFile;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createWriteFile;
import static java.util.Objects.isNull;

public class FIFOFile implements Closeable {
    private FilesManager filesManager;

    private ReadWriteFile writeFile;

    private ReadWriteFile readFile;

    private IndexFile indexFile;

    private long capacity;

    private int maxFileSize;

    @SneakyThrows
    public FIFOFile(@NonNull String folder, long capacity, int maxFileSize) {
        Path path = Paths.get(folder);

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        this.filesManager = FilesManager.builder()
            .folder(path)
            .prefix("queue-")
            .suffix(".log")
            .build();

        this.indexFile = new IndexFile(path);
        this.capacity = capacity;
        this.maxFileSize = maxFileSize;

        initWriteReadFile();
    }

    private void initWriteReadFile() {
        int fileSize = filesManager.getFileNum();
        if (fileSize > 0) {
            long readIndex = indexFile.readIndex(0);
            long writeIndex = indexFile.writeIndex(0);

            Path path = filesManager.last();
            if (readIndex >= writeIndex) {
                writeFile = createWriteFile(path, indexFile, capacity);
            } else {
                writeFile = createWriteFile(path, indexFile, readIndex, writeIndex, capacity);
            }

            if (fileSize > 1) {
                readIndex = indexFile.readIndex(1);
                writeIndex = indexFile.writeIndex(1);

                path = filesManager.first();
                if (readIndex >= writeIndex) {
                    filesManager.remove(path);
                } else {
                    readFile = createReadFile(path, indexFile, readIndex, writeIndex, capacity);
                }
            }
        } else {
            Path path = filesManager.createNextFile();
            writeFile = createWriteFile(path, indexFile, capacity);
        }

    }

    public void put(ByteData byteData) {
        if (!writeFile.write(byteData)) {
            System.out.println(getFileNum() + ":" + getFileSize());
            Path path = filesManager.createNextFile();
            if (filesManager.getFileNum() > maxFileSize) {
                filesManager.remove(readFile.getPath());
                readFile = null;
            }

            if (filesManager.getFileNum() == 1) {
                readFile = createReadFile(writeFile.getPath(), indexFile, writeFile.getReadIndex(), writeFile.getWriteIndex(), capacity);
            }
            writeFile = createWriteFile(path, indexFile, capacity);
            writeFile.write(byteData);
        }
    }

    private ByteData current;

    public ByteData get() {
        if (isNull(current)) {
            next();
        }

        return current;
    }

    public void next() {
        if (isNull(readFile)) {
            if (filesManager.getFileNum() == 1) {
                current = writeFile.read();
                return;
            } else {
                Path path = filesManager.first();
                readFile = createReadFile(path, indexFile, capacity);
            }
        }
        current = readFile.read();
        if (isNull(current)) {
            filesManager.remove(readFile.getPath());
            readFile = null;
        }
    }

    public int getFileNum() {
        return filesManager.getFileNum();
    }

    // for test
    @SneakyThrows
    long getFileSize() {
        return writeFile.getChannel().size();
    }

    @Override
    public void close() {
        safeClose(filesManager);
        safeClose(indexFile);
        safeClose(writeFile);
        safeClose(readFile);
    }
}
