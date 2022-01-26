package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.file.fifo.FilesManager;
import io.appulse.utils.Bytes;
import lombok.*;
import org.apache.commons.lang3.time.StopWatch;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.zmh.fastlog.utils.Utils.*;
import static com.zmh.fastlog.worker.file.LogFileFactory.createReadFile;
import static com.zmh.fastlog.worker.file.LogFileFactory.createWriteFile;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class FIFOQueue implements AutoCloseable, FIFO {

    /**
     * 队列尾巴 是 写缓冲区
     * 使用双内存队列的原因是，当其中一个队列满的时候需要flush到磁盘，这段时间比较长，如果后续日志发送又快的话，会造成日志丢失
     * 所以将flush的操作异步，异步期间使用另外一个内存队列存放日志数据
     */
    private final TwoBytesCacheQueue tail;

    private final LogFileManager logFiles;

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
        logFiles = LogFileManager.builder()
            .folder(folder)
            .cacheSize(cacheSize)
            .maxIndex(10)
            .maxFileNum(maxFileCount)
            .build();

        tail = new TwoBytesCacheQueue(cacheSize);
        head = new BytesCacheQueue(cacheSize);
    }

    public void put(ByteData byteData) {
        if (tail.put(byteData)) {
            return;
        }

        if (head.isEmpty() && logFiles.isEmpty()) {
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

        if (!logFiles.isEmpty()) {
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

/*class LogFiles implements AutoCloseable {

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
                Path poll = files.first();
                files.remove(poll);
                debugLog("remove file：" + poll.getFileName());
            }
        });
    }

    public void pollTo(@NonNull Bytes buffer) {
        do {
            val file = files.first();
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
        return files.getFileNum();
    }

    public int getTotalFile() {
        return files.getIndex().get() - 1;
    }

    @Override
    public void close() {
        files.close();
        singleThreadExecutor.shutdown();
    }

}*/

class LogFileManager implements Closeable {
    private FilesManager filesManager;

    @Getter
    private LogFile writeFile;

    @Getter
    private LogFile readFile;

    private IndexFile indexFile;

    private int maxIndex;

    private int cacheSize;

    private int maxFileNum;

    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();


    @Builder
    @SneakyThrows
    public LogFileManager(String folder, int cacheSize, int maxIndex, int maxFileNum) {
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
        this.maxIndex = maxIndex;
        this.cacheSize = cacheSize;
        this.maxFileNum = maxFileNum;

        initWriteReadFile();
    }

    private void initWriteReadFile() {
        int fileSize = filesManager.getFileNum();
        if (fileSize > 0) {
            int readIndex = indexFile.readIndex(1);
            int writeIndex = indexFile.writeIndex(1);

            Path path = filesManager.last();
            if (readIndex >= writeIndex) {
                writeFile = createWriteFile(path, indexFile, maxIndex, cacheSize);
            } else {
                writeFile = createWriteFile(path, indexFile, readIndex, writeIndex, maxIndex, cacheSize);
            }

            if (fileSize > 1) {
                readIndex = indexFile.readIndex(0);
                writeIndex = indexFile.writeIndex(0);

                path = filesManager.first();
                if (readIndex >= writeIndex) {
                    filesManager.remove(path);
                } else {
                    readFile = createReadFile(path, indexFile, readIndex, writeIndex, maxIndex, cacheSize);
                }
            }
        } else {
            Path path = filesManager.createNextFile();
            writeFile = createWriteFile(path, indexFile, maxIndex, cacheSize);
        }
    }

    @SneakyThrows
    public void pollTo(@NonNull Bytes bytes) {
        if (isNull(readFile)) {
            if (filesManager.getFileNum() == 1) {
                writeFile.pollTo(bytes);
                return;
            } else {
                Path path = filesManager.first();
                readFile = createReadFile(path, indexFile, maxIndex, cacheSize);
            }
        }

        if (!readFile.pollTo(bytes)) {
            filesManager.remove(readFile.getPath());
            readFile = null;
            pollTo(bytes);
        }
    }

    @SneakyThrows
    public Future<?> write(@NonNull Bytes bytes) {
        return singleThreadExecutor.submit(() -> {
            if (writeFile.write(bytes)) {
                return;
            }

            if (filesManager.getFileNum() >= maxFileNum) {
                filesManager.remove(filesManager.first());
                if (nonNull(readFile)) {
                    readFile = null;
                }
            }

            if (filesManager.getFileNum() == 1) {
                readFile = createReadFile(writeFile.getPath(), indexFile, writeFile.getReadIndex(), writeFile.getWriteIndex(), maxIndex, cacheSize);
            }

            Path path = filesManager.createNextFile();
            writeFile = createWriteFile(path, indexFile, maxIndex, cacheSize);
            writeFile.write(bytes);
        });
    }

    public boolean isEmpty() {
        return filesManager.getFileNum() == 0 || (filesManager.getFileNum() == 1 && writeFile.isEmpty());
    }

    public int getFileSize() {
        return filesManager.getFileNum();
    }

    public int getTotalFile() {
        return filesManager.getIndex().get();
    }

    @Override
    public void close() {
        safeClose(filesManager);
        safeClose(writeFile);
        safeClose(readFile);
        safeClose(indexFile);
        singleThreadExecutor.shutdown();
    }
}

class LogFileFactory {
    public static LogFile createWriteFile(Path folder, IndexFile indexFile, int maxIndex, int cacheSize) {
        return createWriteFile(folder, indexFile, 0, 0, maxIndex, cacheSize);
    }

    public static LogFile createWriteFile(Path folder, IndexFile indexFile, int readIndex, int writeIndex, int maxIndex, int cacheSize) {
        indexFile.reset(1, readIndex, writeIndex);
        return new LogFile(folder, indexFile, maxIndex, cacheSize, 1);
    }

    public static LogFile createReadFile(Path folder, IndexFile indexFile, int maxIndex, int cacheSize) {
        return createReadFile(folder, indexFile, 0, maxIndex, maxIndex, cacheSize);
    }

    public static LogFile createReadFile(Path folder, IndexFile indexFile, int readIndex, int writeIndex, int maxIndex, int cacheSize) {
        indexFile.reset(0, readIndex, writeIndex);
        return new LogFile(folder, indexFile, maxIndex, cacheSize, 0);
    }
}

@Getter
class LogFile implements Closeable {
    private Path path;
    private int readIndex;
    private int writeIndex;
    private IndexFile indexFile;
    private FileChannel channel;
    private int fileIndex;
    private int maxIndex;
    private int cacheSize;
    private ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    @SneakyThrows
    public LogFile(Path path, IndexFile indexFile, int maxIndex, int cacheSize, int fileIndex) {
        this.path = path;
        this.indexFile = indexFile;
        this.channel = FileChannel.open(path, WRITE, READ);
        this.fileIndex = fileIndex;
        this.readIndex = indexFile.readIndex(fileIndex);
        this.writeIndex = indexFile.writeIndex(fileIndex);
        this.maxIndex = maxIndex;
        this.cacheSize = cacheSize;
    }

    @SneakyThrows
    public boolean pollTo(@NonNull Bytes bytes) {
        bytes.reset();

        if (readIndex == writeIndex) {
            return false;
        }

        int position = (readIndex & (maxIndex - 1)) * cacheSize;

        lenBuffer.clear();
        channel.read(lenBuffer, position);
        lenBuffer.flip();
        int len = lenBuffer.getInt();

        val byteBuffer = ByteBuffer.wrap(bytes.array());
        byteBuffer.position(0);
        byteBuffer.limit(len);
        channel.read(byteBuffer, position + 4);

        bytes.writerIndex(len);

        readIndex++;
        indexFile.readIndex(fileIndex, readIndex);
        return true;
    }

    @SneakyThrows
    public boolean write(@NonNull Bytes bytes) {
        if (writeIndex - readIndex == maxIndex) {
            return false;
        }

        int position = (writeIndex & (maxIndex - 1)) * cacheSize;

        lenBuffer.clear();
        lenBuffer.putInt(bytes.readableBytes());
        lenBuffer.rewind();
        channel.write(lenBuffer, position);

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes.array());
        byteBuffer.position(0);
        byteBuffer.limit(bytes.readableBytes());
        channel.write(byteBuffer, position + 4);

        writeIndex++;
        indexFile.writeIndex(fileIndex, writeIndex);
        return true;
    }

    public boolean isEmpty() {
        return readIndex == writeIndex;
    }

    @Override
    public void close() {
        safeClose(channel);
    }
}

class IndexFile implements Closeable {

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

    public void writeIndex(int fileIndex, int index) {
        int position = position(fileIndex) + 4;
        mbb.putInt(position, index);
    }

    public void readIndex(int fileIndex, int index) {
        mbb.putInt(position(fileIndex), index);
    }

    public int readIndex(int fileIndex) {
        return mbb.getInt(position(fileIndex));
    }

    public int writeIndex(int fileIndex) {
        return mbb.getInt(position(fileIndex) + 4);
    }

    public void reset(int fileIndex, int readIndex, int writeIndex) {
        int position = position(fileIndex);
        mbb.putInt(position, readIndex);
        mbb.putInt(position + 4, writeIndex);
    }

    private int position(int fileIndex) {
        return (fileIndex & 1) * 8;
    }

    @Override
    public void close() {
        safeClose(raf);
    }
}


/*
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

    public int getFileNum() {
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
*/
