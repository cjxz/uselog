package com.zmh.fastlog.worker.file;

import io.appulse.utils.Bytes;
import lombok.*;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.zmh.fastlog.utils.Utils.safeClose;
import static com.zmh.fastlog.worker.file.LogFileFactory.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;

public class LogFilesManager implements Closeable {

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
    public LogFilesManager(String folder, int cacheSize, int maxIndex, int maxFileNum) {
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
            int readIndex = indexFile.readIndex(WRITE_FILE_INDEX);
            int writeIndex = indexFile.writeIndex(WRITE_FILE_INDEX);

            Path path = filesManager.last();
            if (readIndex >= writeIndex) {
                writeFile = createWriteFile(path, indexFile, maxIndex, cacheSize);
            } else {
                writeFile = createWriteFile(path, indexFile, readIndex, writeIndex, maxIndex, cacheSize);
            }

            if (fileSize > 1) {
                readIndex = indexFile.readIndex(READ_FILE_INDEX);
                writeIndex = indexFile.writeIndex(READ_FILE_INDEX);

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
    public static final int READ_FILE_INDEX = 0;
    public static final int WRITE_FILE_INDEX = 1;

    public static LogFile createWriteFile(Path folder, IndexFile indexFile, int maxIndex, int cacheSize) {
        return createWriteFile(folder, indexFile, 0, 0, maxIndex, cacheSize);
    }

    public static LogFile createWriteFile(Path folder, IndexFile indexFile, int readIndex, int writeIndex, int maxIndex, int cacheSize) {
        indexFile.reset(1, readIndex, writeIndex);
        return new LogFile(folder, indexFile, maxIndex, cacheSize, WRITE_FILE_INDEX);
    }

    public static LogFile createReadFile(Path folder, IndexFile indexFile, int maxIndex, int cacheSize) {
        return createReadFile(folder, indexFile, 0, maxIndex, maxIndex, cacheSize);
    }

    public static LogFile createReadFile(Path folder, IndexFile indexFile, int readIndex, int writeIndex, int maxIndex, int cacheSize) {
        indexFile.reset(0, readIndex, writeIndex);
        return new LogFile(folder, indexFile, maxIndex, cacheSize, READ_FILE_INDEX);
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

    public boolean pollTo(@NonNull Bytes bytes) {
        try {
            return pollToEx(bytes);
        } catch (BufferOverflowException exception) {
            System.out.println(exception.getMessage());
        }
        return false;
    }

    @SneakyThrows
    public boolean pollToEx(@NonNull Bytes bytes) {
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

class FilesManager implements AutoCloseable {

    @Getter
    private final AtomicInteger index;

    private final LinkedList<Path> queue;

    private final Path folder;

    private final String prefix;

    private final String suffix;

    private final Pattern fileNamePattern;

    private final Pattern fileIndexPattern;

    @Builder
    public FilesManager(@NonNull Path folder, String prefix, String suffix) {
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
    LinkedList<Path> getFilesFromFileSystem() {
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

    public Path getFile(int fileIndex) {
        val fileName = String.format(ENGLISH, "%s%d%s", prefix, fileIndex, suffix);
        return folder.resolve(fileName);
    }

    @SneakyThrows
    public Path createNextFile() {
        Path result;
        do {
            val nextIndex = index.getAndIncrement();
            result = getFile(nextIndex);
        } while (Files.exists(result));

        Files.createFile(result);
        this.queue.add(result);
        return result;
    }

    public Path first() {
        return queue.peekFirst();
    }

    public Path last() {
        return queue.peekLast();
    }

    @SneakyThrows
    public void remove(@NonNull Path... paths) {
        for (val path : paths) {
            Files.deleteIfExists(path);
            queue.remove(path);
        }
    }
}
