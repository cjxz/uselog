package com.zmh.fastlog.worker.backend;

import com.zmh.fastlog.config.WalFilesConfig;
import com.zmh.fastlog.worker.backend.exceptions.CorruptedDataException;
import io.appulse.utils.Bytes;
import io.appulse.utils.ReadBytesUtils;
import io.appulse.utils.WriteBytesUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.io.FileOutputStream;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static lombok.AccessLevel.PRIVATE;

@FieldDefaults(level = PRIVATE, makeFinal = true)
public class LogFiles implements AutoCloseable {

    FilesManager files;

    int maxCount;

    Function<CorruptedDataException, Boolean> corruptionHandler;

    @Builder
    LogFiles(@NonNull String queueName,
             @NonNull WalFilesConfig config,
             Boolean restoreFromDisk,
             Function<CorruptedDataException, Boolean> corruptionHandler
    ) {
        val restoreFromDiskValue = ofNullable(restoreFromDisk)
            .orElse(Boolean.TRUE);

        val corruptionHandlerValue = ofNullable(corruptionHandler)
            .orElseGet(DefaultCorruptionHandler::new);

        files = FilesManager.builder()
            .folder(config.getFolder())
            .prefix(queueName + '-')
            .suffix(".log")
            .build();

        if (!restoreFromDiskValue) {
            files.clear();
        }

        maxCount = config.getMaxCount();
        this.corruptionHandler = corruptionHandlerValue;
    }

    @SneakyThrows
    public void write(@NonNull Bytes buffer) {
        val file = files.createNextFile();
        WriteBytesUtils.write(file, buffer);

        Snappy.compress(buffer.array());

        if (isLimitExceeded()) {
            files.remove(files.poll());
        }
    }

    public void pollTo(@NonNull Bytes buffer) {
        readTo(buffer);
    }

    public int getFileSize() {
        return files.getFilesFromQueue().size();
    }

    boolean isLimitExceeded() {
        return getFileSize() > maxCount;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private void readTo(Bytes buffer) {
        val writerIndex = buffer.writerIndex();
        val readerIndex = buffer.readerIndex();
        do {
            val file = files.poll();
            try {
                if (file == null) {
                    return;
                }

                val length = ReadBytesUtils.read(file, buffer);
                if (length > 0) {
                    files.remove(file);
                }
                return;
            } catch (Exception ex) {
                val exception = new CorruptedDataException(file, 0, ex);
                corruptionHandler.apply(exception);
                files.remove(file);
            }
            buffer.writerIndex(writerIndex);
            buffer.readerIndex(readerIndex);
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

