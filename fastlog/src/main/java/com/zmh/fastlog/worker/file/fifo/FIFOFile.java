package com.zmh.fastlog.worker.file.fifo;

import com.zmh.fastlog.model.message.ByteData;
import lombok.NonNull;

import java.nio.file.Path;

import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createReadFile;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createWriteFile;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

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

        this.indexFile = new IndexFile(folder);
        this.capacity = capacity;
        this.maxFileSize = maxFileSize;
    }

    private void initWriteReadFile() {
        int fileSize = filesManager.getFileSize();
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
        }

    }

    public void write(ByteData byteData) {
        if (isNull(writeFile) || !writeFile.write(byteData)) {
            Path path = filesManager.createNextFile();
            if (filesManager.getFileSize() > maxFileSize) {
                filesManager.remove(readFile.getPath());
                readFile = null;
            }

            if (isNull(readFile) && nonNull(writeFile) && filesManager.getFileSize() == 1) {
                readFile = createReadFile(path, indexFile, writeFile.getReadIndex(), writeFile.getWriteIndex(), capacity);
            }
            writeFile = createWriteFile(path, indexFile, capacity);
        }

        writeFile.write(byteData);
    }

    public ByteData read() {
        if (isNull(readFile)) {
            if (filesManager.getFileSize() == 1) {
                return writeFile.read();
            } else {
                Path path = filesManager.first();
                readFile = createReadFile(path, indexFile, capacity);
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
