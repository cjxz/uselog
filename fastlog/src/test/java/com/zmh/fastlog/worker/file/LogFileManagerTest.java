package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.worker.BeforeDeleteFile;
import io.appulse.utils.Bytes;
import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.*;

public class LogFileManagerTest extends BeforeDeleteFile {

    private static final int CACHE_SIZE = 256;

    @Test
    @SneakyThrows
    public void writeAndReadSingleTimeTest() {
        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            Bytes bytes = getBytes();
            int length = bytes.readableBytes();

            manager.write(bytes).get();
            assertEquals(1, manager.getFileSize());

            manager.pollTo(bytes);
            assertEquals(1, manager.getFileSize());
            assertEquals(length, bytes.readableBytes());

            assertTrue(manager.isEmpty());
        }
    }

    @Test
    @SneakyThrows
    public void writeAndReadManyTimeInOneFileTest() {
        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            Bytes bytes = getBytes();
            int length = bytes.readableBytes();

            for (int i = 0; i < 4; i++) {
                manager.write(bytes).get();
            }
            assertEquals(1, manager.getFileSize());

            for (int i = 0; i < 4; i++) {
                manager.pollTo(bytes);
                assertEquals(length, bytes.readableBytes());
            }
            assertEquals(1, manager.getFileSize());

            assertTrue(manager.isEmpty());
        }
    }

    @Test
    @SneakyThrows
    public void writeAndReadManyTimeInManyFileTest() {
        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            Bytes bytes = getBytes();
            int length = bytes.readableBytes();

            for (int i = 0; i < 10; i++) {
                manager.write(bytes).get();
                manager.pollTo(bytes);
            }
            assertEquals(1, manager.getFileSize());

            for (int i = 0; i < 10; i++) {
                manager.write(bytes).get();
            }
            assertEquals(3, manager.getFileSize());
            assertEquals(10, manager.getReadFile().getReadIndex());
            assertEquals(14, manager.getReadFile().getWriteIndex());

            for (int i = 0; i < 5; i++) {
                manager.pollTo(bytes);
                assertEquals(length, bytes.readableBytes());
            }
            assertEquals(2, manager.getFileSize());
            assertEquals(3, manager.getTotalFile());
        }
    }

    @Test
    @SneakyThrows
    public void initWriteReadFileTest() {
        Bytes bytes = getBytes();
        int length = bytes.readableBytes();

        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            for (int i = 0; i < 10; i++) {
                manager.write(bytes).get();
            }
        }

        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            assertNotNull(manager.getWriteFile());
            assertNotNull(manager.getReadFile());
            assertEquals(3, manager.getFileSize());

            for (int i = 0; i < 10; i++) {
                manager.pollTo(bytes);
                assertEquals(length, bytes.readableBytes());
            }

            manager.pollTo(bytes);
            assertEquals(0, bytes.readableBytes());
        }
    }

    @Test
    @SneakyThrows
    public void initWriteReadFileTest2() {
        Bytes bytes = getBytes();
        int length = bytes.readableBytes();

        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            for (int i = 0; i < 10; i++) {
                manager.write(bytes).get();
            }

            for (int i = 0; i < 4; i++) {
                manager.pollTo(bytes);
            }
            assertEquals(3, manager.getFileSize());
        }

        try (LogFileManager manager = new LogFileManager(FOLDER, CACHE_SIZE, 4, 10)) {
            assertNotNull(manager.getWriteFile());
            assertNull(manager.getReadFile());
            assertEquals(2, manager.getFileSize());

            for (int i = 0; i < 4; i++) {
                manager.pollTo(bytes);
                assertEquals(length, bytes.readableBytes());
            }
        }
    }

    private Bytes getBytes() {
        Bytes bytes = Bytes.allocate(CACHE_SIZE);
        byte[] array = "全世界都在说中国话1234567890abcdefghijklmnopqrstuvwsyzABCDEFGHIJKLMNOPQRSTUVWSYZ~!@#$%^&*()_+{}|:\"<>?全世界都在说中国话1234567890abcdefghijklmnopqrstuvwsyzABCDEFGHIJKLMNOPQRSTUVWSYZ~!@#$%^&*()_+{}|:\"<>?".getBytes();
        System.out.println(array.length);
        bytes.writeNB(array);
        return bytes;
    }
}
