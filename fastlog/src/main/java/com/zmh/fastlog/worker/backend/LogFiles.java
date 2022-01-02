package com.zmh.fastlog.worker.backend;

import com.zmh.fastlog.config.WalFilesConfig;
import com.zmh.fastlog.worker.backend.exceptions.CorruptedDataException;
import io.appulse.utils.Bytes;
import io.appulse.utils.ReadBytesUtils;
import io.appulse.utils.WriteBytesUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;

public class LogFiles implements AutoCloseable {

    private static final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    private final FilesManager files;

    private final int maxCount;

    private final Function<CorruptedDataException, Boolean> corruptionHandler;

    @Builder
    LogFiles(@NonNull String queueName,
             @NonNull WalFilesConfig config,
             @NonNull int cacheSize,
             Function<CorruptedDataException, Boolean> corruptionHandler
    ) {
        val corruptionHandlerValue = ofNullable(corruptionHandler)
            .orElseGet(DefaultCorruptionHandler::new);

        files = FilesManager.builder()
            .folder(config.getFolder())
            .prefix(queueName + '-')
            .suffix(".log")
            .build();

        maxCount = config.getMaxCount();
        this.corruptionHandler = corruptionHandlerValue;
        this.compressBytes = Bytes.allocate(cacheSize);
        this.decompressBytes = Bytes.allocate(cacheSize);
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        this.lz4Compressor = lz4Factory.fastCompressor();
        lz4FastDecompressor = lz4Factory.fastDecompressor();
    }

    private Bytes compressBytes;
    private Bytes decompressBytes; //todo zmh 如果不够用，则再加长
    private LZ4Compressor lz4Compressor;
    private LZ4FastDecompressor lz4FastDecompressor;

    // 正在执行中的写磁盘任务
    private Future<?> future; // todo zmh 命名


    @SneakyThrows
    public void write(@NonNull Bytes buffer) {
        StopWatch stopWatch = new StopWatch();

        if (nonNull(future) && !future.isDone()) {
            stopWatch.start();
            future.get();
            stopWatch.stop();

            System.out.println("wait future:" + stopWatch.formatTime());
        }

        int after = lz4Compressor.compress(buffer.array(), buffer.readerIndex(), buffer.writerIndex(), compressBytes.array(), 0, compressBytes.capacity());
        System.out.println("compress，" + "before:" + buffer.readableBytes() + "，after" + after);
        compressBytes.writerIndex(after);


        this.future = singleThreadExecutor.submit(() -> {
            stopWatch.reset();
            stopWatch.start();
            val file = files.createNextFile();
            WriteBytesUtils.write(file, compressBytes); //todo zmh 转成bytebuffer
            files.addFile(file);
            stopWatch.stop();
            System.out.println("file:" + stopWatch.formatTime());
            compressBytes.reset();

            if (isLimitExceeded()) {
                files.remove(files.poll());
            }
        });
    }

    public void pollTo(@NonNull Bytes buffer) {
        readTo(buffer);
    }

    public int getFileSize() {
        return files.getFilesFromQueue().size();
    }

    private boolean isLimitExceeded() {
        return getFileSize() > maxCount;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private void readTo(Bytes buffer) {
        do {
            val file = files.poll();
            try {
                if (file == null) {
                    return;
                }

                val length = ReadBytesUtils.read(file, decompressBytes);
                if (length > 0) {
                    int decompressLength = lz4FastDecompressor.decompress(decompressBytes.array(), buffer.array());
                    buffer.writerIndex(decompressLength);
                    decompressBytes.reset();

                    files.remove(file);
                }
                return;
            } catch (Exception ex) {
                val exception = new CorruptedDataException(file, 0, ex);
                corruptionHandler.apply(exception);
                files.remove(file);
            }
        } while (true);
    }

    @Override
    public void close() {
        files.close();
    }

    @Slf4j
    public static class DefaultCorruptionHandler implements Function<CorruptedDataException, Boolean> {

        @Override
        public Boolean apply(CorruptedDataException exception) {
            log.error("Corrupted data error", exception);
            return Boolean.TRUE;
        }
    }

}
