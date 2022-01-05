package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.FileMqMessage;
import com.zmh.fastlog.model.event.ByteEvent;
import com.zmh.fastlog.utils.Utils;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import io.appulse.utils.Bytes;
import io.appulse.utils.ReadBytesUtils;
import io.appulse.utils.WriteBytesUtils;
import lombok.*;
import org.apache.commons.lang3.time.StopWatch;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;

public class FIFOQueue implements AutoCloseable {

    private final TwoBytesCacheQueue tail;

    private final LogFiles logFiles;

    private final BytesCacheQueue head;

    @SneakyThrows
    FIFOQueue(int cacheSize, String folder) {
        Path path = Paths.get(folder);

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        logFiles = LogFiles.builder()
            .queueName("queue")
            .folder(path)
            .maxFileCount(cacheSize)
            .build();

        tail = new TwoBytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
    }

    public void put(ByteEvent byteBuffer) {
        if (tail.put(byteBuffer)) {
            return;
        }

        if (head.isEmpty() && fileSize() == 0) {
            tail.copyTo(head);
            tail.reset();
        } else {
            flush();
            tail.switchHead();
        }

        tail.put(byteBuffer);
    }

    private AbstractMqMessage message;

    public AbstractMqMessage get() {
        if (nonNull(message)) {
            return message;
        }

        message = head.get();
        if (nonNull(message)) {
            return message;
        }

        if (fileSize() > 0) {
            logFiles.pollTo(head.getBytes());
            message = head.get();
            return message;
        }

        message = tail.get();
        return message;
    }

    public void next() {
        message = null;
    }

    // for test
    int fileSize() {
        return logFiles.getFileSize();
    }

    public void flush() {
        if (tail.isEmpty()) {
            return;
        }
        logFiles.write(tail.getUsed());
    }

    @Override
    public void close() {
        flush();
        logFiles.close();
    }

}

class TwoBytesCacheQueue {
    @Getter
    private BytesCacheQueue used;
    private BytesCacheQueue other;

    public TwoBytesCacheQueue(int size) {
        used = new BytesCacheQueue(size);
        other = new BytesCacheQueue(size);
    }

    public void switchHead() {
        other.checkPut();
        other.reset();
        BytesCacheQueue temp = used;
        used = other;
        other = temp;
    }

    public boolean put(ByteEvent event) {
        return used.put(event);
    }

    public FileMqMessage get() {
        return used.get();
    }

    public void reset() {
        this.used.reset();
    }

    public boolean isEmpty() {
        return this.used.isEmpty();
    }

    public void copyTo(BytesCacheQueue queue) {
        this.used.copyTo(queue);
    }
}

class BytesCacheQueue {
    @Getter
    private final Bytes bytes;

    private Future<?> future;

    public BytesCacheQueue(int size) {
        this.bytes = Bytes.allocate(size);
    }

    public boolean put(ByteEvent event) {
        ByteBuffer bb = event.getBuffer();
        val writerIndex = this.bytes.writerIndex();

        if (writerIndex + Long.BYTES + Integer.BYTES + bb.limit() > bytes.capacity()) {
            return false;
        }

        this.bytes.write4B(0); // write fake length
        this.bytes.write8B(event.getId());
        this.bytes.writeNB(bb.array());

        this.bytes.set4B(writerIndex, this.bytes.writerIndex() - writerIndex - Integer.BYTES - Long.BYTES); // write real length
        return true;
    }

    private byte[] readBuffer = new byte[5120];

    public FileMqMessage get() {
        if (bytes.readableBytes() == 0) {
            this.bytes.reset();
            return null;
        }
        int readCount = this.bytes.readInt();
        if (readCount > 0) {
            if (readCount > readBuffer.length) {
                readBuffer = new byte[marginToBuffer(readCount)];
            }
            long id = this.bytes.readLong();
            this.bytes.readBytes(readBuffer, 0, readCount);

            return new FileMqMessage(id, readBuffer, readCount);
        } else {
            return null; //todo zmh throw exception
        }
    }

    public void asyncToDisk(Future<?> future) {
        this.future = future;
    }

    @SneakyThrows
    public void checkPut() {
        if (isNull(future) || future.isDone()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        future.get();
        stopWatch.stop();
        System.out.println("wait future:" + stopWatch.formatTime());
    }

    public void reset() {
        this.bytes.reset();
    }

    public boolean isEmpty() {
        return bytes.readableBytes() == 0;
    }

    public void copyTo(BytesCacheQueue queue) {
        queue.reset();

        queue.bytes.writeNB(this.bytes.array(), this.bytes.readerIndex(), this.bytes.readableBytes());
    }

}

class LogFiles implements AutoCloseable {

    private static final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private final FilesManager files;

    private final int maxCount;

    @Builder
    LogFiles(@NonNull String queueName,
             @NonNull Path folder,
             @NonNull int maxFileCount
    ) {
        this.files = FilesManager.builder()
            .folder(folder)
            .prefix(queueName + '-')
            .suffix(".log")
            .build();

        this.maxCount = maxFileCount;
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

class FilesManager implements AutoCloseable {

    private final AtomicInteger index;

    private final Queue<Path> queue;

    private final Path folder;

    private final String prefix;

    private final String suffix;

    private final Pattern fileNamePattern;

    private final Pattern fileIndexPattern;

    @Builder
    FilesManager(@NonNull Path folder, String prefix, String suffix) {
        index = new AtomicInteger(0);

        this.folder = folder;
        this.prefix = ofNullable(prefix)
            .map(String::trim)
            .orElse("");
        this.suffix = ofNullable(suffix)
            .map(String::trim)
            .orElse("");

        val fileNameRegex = String.format(ENGLISH, "^%s\\d+%s$", this.prefix, this.suffix);
        fileNamePattern = Pattern.compile(fileNameRegex);

        val fileIndexRegex = String.format(ENGLISH, "^%s(?<index>\\d+)%s$", this.prefix, this.suffix);
        fileIndexPattern = Pattern.compile(fileIndexRegex);

        queue = getFilesFromFileSystem();
        if (!queue.isEmpty()) {
            val array = queue.toArray(new Path[0]);
            Path lastPath = array[array.length - 1];
            val lastPathIndex = getIndex(lastPath);
            index.set(lastPathIndex + 1);
        }
    }

    @Override
    public void close() {
        queue.clear();
    }

    Queue<Path> getFilesFromQueue() {
        return queue;
    }

    @SneakyThrows
    Queue<Path> getFilesFromFileSystem() {
        val file = folder.toFile();
        val list = file.list();
        if (list != null && list.length == 0) {
            return new LinkedList<>();
        }

        return Files.list(folder)
            .filter(Files::isRegularFile)
            .filter(path -> ofNullable(path)
                .map(Path::getFileName)
                .map(Path::toString)
                .map(fileNamePattern::matcher)
                .map(Matcher::matches)
                .orElse(false)
            )
            .sorted(comparing(this::getIndex))
            .collect(toCollection(LinkedList::new));
    }

    int getIndex(@NonNull Path path) {
        return of(path)
            .map(Path::getFileName)
            .map(Path::toString)
            .map(fileIndexPattern::matcher)
            .filter(Matcher::find)
            .map(matcher -> matcher.group("index"))
            .map(Integer::valueOf)
            .orElseThrow(() -> {
                val msg = String.format(ENGLISH, "File '%s' doesn't have index group", path.toString());
                return new IllegalArgumentException(msg);
            });
    }

    Path getFile(int fileIndex) {
        val fileName = String.format(ENGLISH, "%s%d%s", prefix, fileIndex, suffix);
        return folder.resolve(fileName);
    }

    @SneakyThrows
    Path createNextFile() {
        Path result;
        do {
            val nextIndex = index.getAndIncrement();
            result = getFile(nextIndex);
        } while (Files.exists(result));

        Files.createFile(result);
        this.queue.add(result);
        return result;
    }

    Path poll() {
        return queue.poll();
    }

    Path peek() {
        return queue.peek();
    }

    @SneakyThrows
    void remove(@NonNull Path... paths) {
        for (val path : paths) {
            Files.deleteIfExists(path);
            queue.remove(path);
        }
    }
}
