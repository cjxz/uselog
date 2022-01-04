package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.config.WalFilesConfig;
import com.zmh.fastlog.utils.Utils;
import io.appulse.utils.Bytes;
import io.appulse.utils.ReadBytesUtils;
import io.appulse.utils.WriteBytesUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang3.time.StopWatch;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Objects.nonNull;

public class LogFiles implements AutoCloseable {

    private static final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private final FilesManager files;

    private final int maxCount;

    @Builder
    LogFiles(@NonNull String queueName,
             @NonNull WalFilesConfig config,
             @NonNull int cacheSize
    ) {
        this.files = FilesManager.builder()
            .folder(config.getFolder())
            .prefix(queueName + '-')
            .suffix(".log")
            .build();

        this.maxCount = config.getMaxCount();
    }

    @SneakyThrows
    public void write(@NonNull BytesCacheQueue bytesCacheQueue) {
        Bytes bytes = bytesCacheQueue.getBytes();

        Future<?> future = singleThreadExecutor.submit(() -> {
            StopWatch stopWatch = new StopWatch();
            stopWatch.reset();
            stopWatch.start();
            val file = files.createNextFile();
            WriteBytesUtils.write(file, bytes);
            stopWatch.stop();
            System.out.println("wait file：" + stopWatch.formatTime());

            if (isLimitExceeded()) {
                Path poll = files.poll();
                files.remove(poll);
                System.out.println("remove file：" + poll.getFileName());
            }
        });
        bytesCacheQueue.asyncToDisk(future);
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
            val file = files.peek();
            try {
                if (file == null) {
                    return;
                }

                buffer.reset();
                val length = ReadBytesUtils.read(file, buffer);
                if (length > 0) {
                    files.remove(file);
                } else {
                    System.out.println("read empty file");
                }
                return;
            } catch (Exception ex) {
                Utils.debugLog("read data error " + ex);
                files.remove(file);
            }
        } while (true);
    }

    @Override
    public void close() {
        files.close();
    }

}
