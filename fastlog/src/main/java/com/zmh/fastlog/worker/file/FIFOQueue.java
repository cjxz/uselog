package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import io.appulse.utils.Bytes;
import io.appulse.utils.ReadBytesUtils;
import io.appulse.utils.WriteBytesUtils;
import lombok.*;
import org.apache.commons.lang3.time.StopWatch;

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

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.marginToBuffer;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;

public class FIFOQueue implements AutoCloseable, FIFO {

    /**
     * 队列尾巴 是 写缓冲区
     * 使用双内存队列的原因是，当其中一个队列满的时候需要flush到磁盘，这段时间比较长，如果后续日志发送又快的话，会造成日志丢失
     * 所以将flush的操作异步，异步期间使用另外一个内存队列存放日志数据
     */
    private final TwoBytesCacheQueue tail;

    private final LogFiles logFiles;

    /**
     * 队列头 是 读缓冲区
     * 队列头的数据来源有两种场景：
     * 1、如果日志还没有多到需要放入磁盘的话，会将写缓冲区的日志数据直接复制队列头，这样写缓冲区可以继续写，读缓冲区可以从内存中读取，
     *    注意：这里的写缓冲区的数据不一定是满的，因为当日志的写入和读取速度相当的时候，日志可以直接从写缓冲区中获取，而不一定是非得从读缓冲区中获取
     * 2、如果日志多到已经写入磁盘，那最早的日志数据一定在磁盘文件，此时需要从磁盘中读取文件写入该读缓冲区，供后续读取日志使用
     */
    private final BytesCacheQueue head;

    @SneakyThrows
    FIFOQueue(String folder, int cacheSize, int maxFileCount) {
        Path path = Paths.get(folder);

        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        logFiles = LogFiles.builder()
            .queueName("queue")
            .folder(path)
            .maxFileCount(maxFileCount)
            .build();

        tail = new TwoBytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
    }

    public void put(ByteData byteData) {
        if (tail.put(byteData)) {
            return;
        }

        if (head.isEmpty() && getFileNum() == 0) {
            tail.copyTo(head);
            tail.reset();
        } else {
            flush();
        }

        tail.put(byteData);
    }

    private ByteData current;

    public ByteData get() {
        if (isNull(current)) {
            next();
        }
        return current;
    }

    public void next() {
        current = head.get();
        if (nonNull(current)) {
            return;
        }

        if (getFileNum() > 0) {
            logFiles.pollTo(head.getBytes());
            current = head.get();
            return;
        }

        current = tail.get();
    }

    // for test
    public int getFileNum() {
        return logFiles.getFileSize();
    }

    @Override
    public int getTotalFile() {
        return logFiles.getTotalFile();
    }

    public void flush() {
        if (tail.isEmpty()) {
            return;
        }

        tail.waitFutureDone();
        Future<?> future = logFiles.write(tail.getBytes());
        tail.flush(future);
    }

    @Override
    public void close() {
        flush();
        logFiles.close();
    }

}

class TwoBytesCacheQueue {

    private BytesCacheQueueFlush used;
    private BytesCacheQueueFlush other;

    public TwoBytesCacheQueue(int size) {
        used = new BytesCacheQueueFlush(size);
        other = new BytesCacheQueueFlush(size);
    }

    public void flush(Future<?> future) {
        this.used.flush(future);
        switchQueue();
    }

    public void waitFutureDone() {
        this.other.waitFutureDone();
    }

    private void switchQueue() {
        other.getQueue().reset();
        BytesCacheQueueFlush temp = used;
        used = other;
        other = temp;
    }

    public boolean put(ByteData byteData) {
        return this.used.getQueue().put(byteData);
    }

    public ByteData get() {
        return this.used.getQueue().get();
    }

    public void reset() {
        this.used.getQueue().reset();
    }

    public boolean isEmpty() {
        return this.used.getQueue().isEmpty();
    }

    public void copyTo(BytesCacheQueue queue) {
        this.used.getQueue().copyTo(queue);
    }

    public Bytes getBytes() {
        return this.used.getQueue().getBytes();
    }
}

class BytesCacheQueueFlush {
    @Getter
    private final BytesCacheQueue queue;
    private Future<?> future;

    public BytesCacheQueueFlush(int size) {
        this.queue = new BytesCacheQueue(size);
    }

    public void flush(Future<?> future) {
        this.future = future;
    }

    @SneakyThrows
    public void waitFutureDone() {
        if (isNull(future) || future.isDone()) {
            return;
        }
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        future.get();
        stopWatch.stop();
        debugLog("wait future:" + stopWatch.formatTime());
    }
}

class BytesCacheQueue {
    @Getter
    private final Bytes bytes;

    public BytesCacheQueue(int size) {
        this.bytes = Bytes.allocate(size);
    }

    public boolean put(ByteData byteData) {
        int dataLength = byteData.getDataLength();
        if (this.bytes.writerIndex() + Long.BYTES + Integer.BYTES + dataLength > bytes.capacity()) {
            return false;
        }

        this.bytes.write4B(dataLength); //日志的长度 单位：字节
        this.bytes.write8B(byteData.getId());
        this.bytes.writeNB(byteData.getData(), 0, dataLength);
        return true;
    }

    private byte[] readBuffer = new byte[5120];

    public ByteData get() {
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

            return new ByteData(id, readBuffer, readCount);
        } else {
            debugLog("fastlog BytesCacheQueue readCount error " + readCount);
            this.bytes.reset();
            return null;
        }
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
    public Future<?> write(@NonNull Bytes bytes) {
         return singleThreadExecutor.submit(() -> {
            StopWatch stopWatch = new StopWatch();
            stopWatch.reset();
            stopWatch.start();
            val file = files.createNextFile();
            WriteBytesUtils.write(file, bytes);
            stopWatch.stop();
            debugLog("wait file：" + stopWatch.formatTime());

            if (getFileSize() > maxCount) {
                Path poll = files.poll();
                files.remove(poll);
                debugLog("remove file：" + poll.getFileName());
            }
        });
    }

    public void pollTo(@NonNull Bytes buffer) {
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
                    // 异步flush到磁盘时，已经将file对应的path放入files中，但是该file还未写入磁盘成功，此时会读取失败，属于正常现场，需要下次再来读取
                    debugLog("read empty file");
                }
                return;
            } catch (Exception ex) {
                debugLog("read data error " + ex);
                files.remove(file);
            }
        } while (true);
    }

    public int getFileSize() {
        return files.getFileSize();
    }

    public int getTotalFile() {
        return files.getIndex().get() - 1;
    }

    @Override
    public void close() {
        files.close();
        singleThreadExecutor.shutdown();
    }

}

class FilesManager implements AutoCloseable {

    @Getter
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

    public int getFileSize() {
        return queue.size();
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
