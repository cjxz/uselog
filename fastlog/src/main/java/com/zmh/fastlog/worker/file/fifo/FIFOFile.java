package com.zmh.fastlog.worker.file.fifo;

import io.appulse.utils.Bytes;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.pulsar.shade.io.airlift.compress.Compressor;
import org.apache.pulsar.shade.io.airlift.compress.Decompressor;
import org.apache.pulsar.shade.io.airlift.compress.lz4.Lz4Compressor;
import org.apache.pulsar.shade.io.airlift.compress.lz4.Lz4Decompressor;
import org.apache.pulsar.shade.io.airlift.compress.snappy.SnappyCompressor;
import org.apache.pulsar.shade.io.airlift.compress.snappy.SnappyDecompressor;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.zmh.fastlog.utils.Utils.*;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createReadFile;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createWriteFile;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class FIFOFile implements Closeable {
    private FilesManager filesManager;

    private ReadWriteFile writeFile;

    private ReadWriteFile readFile;

    private IndexFile indexFile;

    private long capacity;

    private int maxFileSize;

    private Compressor compressor;
    private Decompressor decompressor;

    private byte[] compressorBuffer;

    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    @SneakyThrows
    public FIFOFile(@NonNull String folder, int cacheSize, long capacity, int maxFileSize) {
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

        compressor = new SnappyCompressor();
        decompressor = new SnappyDecompressor();
        compressorBuffer = new byte[cacheSize + cacheSize / 255 + 20];
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

    public Future<?> write(Bytes bytes) {
        return singleThreadExecutor.submit(() -> {
            try {
                //先压缩
                int compress = compressor.compress(bytes.array(), 0, bytes.readableBytes(), compressorBuffer, 0, compressorBuffer.length);
                debugLogCondition("compress, before" + bytes.readableBytes() + ",after" + compress);

                ByteBuffer buffer = ByteBuffer.wrap(compressorBuffer);
                buffer.position(0);
                buffer.limit(compress);

                if (writeFile.write(buffer)) {
                    return;
                }
                if (filesManager.getFileNum() == 1) {
                    readFile = createReadFile(writeFile.getPath(), indexFile, writeFile.getReadIndex(), writeFile.getWriteIndex(), capacity);
                }

                Path path = filesManager.createNextFile();

                debugLog("new file path" + path.toString());

                if (filesManager.getFileNum() > maxFileSize) {
                    filesManager.remove(filesManager.first());
                    if (nonNull(readFile)) {
                        readFile = null;
                    }
                }

                writeFile = createWriteFile(path, indexFile, capacity);
                writeFile.write(buffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public Future<?> pollTo(Bytes bytes) {
        return singleThreadExecutor.submit(() -> {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(compressorBuffer);

                if (pollTo(buffer)) {
                    int decompress = decompressor.decompress(buffer.array(), 0, buffer.limit(), bytes.array(), 0, bytes.array().length);
                    debugLogCondition("decompress, before" + buffer.limit() + ",after" + decompress);

                    bytes.readerIndex(0);
                    bytes.writerIndex(decompress);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private boolean pollTo(ByteBuffer buffer) {
        if (isNull(readFile)) {
            if (filesManager.getFileNum() == 1) {
                return writeFile.pollTo(buffer);
            } else {
                Path path = filesManager.first();
                readFile = createReadFile(path, indexFile, capacity);
            }
        }

        if (!readFile.pollTo(buffer)) {
            filesManager.remove(readFile.getPath());
            readFile = null;
            return pollTo(buffer);
        }
        return true;
    }

    public int getFileSize() {
        return filesManager.getFileNum();
    }

    public int getTotalFile() {
        return filesManager.getIndex(filesManager.last());
    }

    public boolean isEmpty() {
        return filesManager.getFileNum() == 0 || (filesManager.getFileNum() == 1 && writeFile.isEmpty());
    }

    @Override
    public void close() {
        safeClose(filesManager);
        safeClose(indexFile);
        safeClose(writeFile);
        safeClose(readFile);
    }
}
