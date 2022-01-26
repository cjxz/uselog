package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.worker.BeforeDeleteFile;
import io.appulse.utils.Bytes;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.zmh.fastlog.worker.file.LogFileFactory.createWriteFile;
import static org.junit.Assert.*;

public class LogFileTest extends BeforeDeleteFile {
    private static final int CACHE_SIZE = 256;

    private Path file;
    private IndexFile indexFile;

    @Before
    @SneakyThrows
    public void before() {
        Path path = Paths.get(FOLDER);
        Files.createDirectories(path);
        this.indexFile = new IndexFile(path);

        this.file = Paths.get(FOLDER + "/queue.log");
        Files.createFile(file);
    }

    @Test
    @SneakyThrows
    public void writeAndReadSingleTimeTest() {
        try (LogFile logFile = createWriteFile(file, indexFile, 10, CACHE_SIZE)) {
            Bytes bytes = Bytes.allocate(CACHE_SIZE);
            byte[] array = "1234567890abcdefghijklmnopqrstuvwsyz优惠库存包括活动商品库存、优惠券库存、权益包库存，只有一个数量的概念，其中活动商品库存是从OMS系统中先拿一部分数量出来给活动商品库存".getBytes();
            bytes.writeNB(array);

            logFile.write(bytes);
            assertEquals(1, logFile.getWriteIndex());
            assertEquals(0, logFile.getReadIndex());

            logFile.pollTo(bytes);
            assertEquals(1, logFile.getReadIndex());
            assertEquals(array.length, bytes.readableBytes());
            assertTrue(logFile.isEmpty());
        }
    }

    @Test
    @SneakyThrows
    public void writeAndReadManyTimeTest() {
        try (LogFile logFile = createWriteFile(file, indexFile, 10, CACHE_SIZE)) {
            Bytes bytes = Bytes.allocate(CACHE_SIZE);
            byte[] array = "1234567890abcdefghijklmnopqrstuvwsyz优惠库存包括活动商品库存、优惠券库存、权益包库存，只有一个数量的概念，其中活动商品库存是从OMS系统中先拿一部分数量出来给活动商品库存".getBytes();
            bytes.writeNB(array);

            for (int i = 0; i < 10; i++) {
                boolean success = logFile.write(bytes);
                assertTrue(success);
            }
            assertEquals(10, logFile.getWriteIndex());
            assertEquals(0, logFile.getReadIndex());

            boolean success = logFile.write(bytes);
            assertFalse(success);

            for (int i = 0; i < 10; i++) {
                success = logFile.pollTo(bytes);
                assertEquals(array.length, bytes.readableBytes());
                assertTrue(success);
            }
            assertEquals(10, logFile.getReadIndex());
            assertTrue(logFile.isEmpty());

            success = logFile.pollTo(bytes);
            assertFalse(success);
        }
    }
}
