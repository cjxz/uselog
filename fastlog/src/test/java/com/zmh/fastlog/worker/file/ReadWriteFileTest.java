package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ReadWriteFileTest extends BeforeDeleteFile {

    @Test
    @SneakyThrows
    public void testWriteAndGetSingleTime() {
        Path logFile = Paths.get("logs/cache/index-1.log");
        Files.createFile(logFile);

        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 1);
        try (ReadWriteFile rwf = new ReadWriteFile(logFile, indexFile, 1 << 20, 1)) {
            byte[] bytes = new byte[128];
            Arrays.fill(bytes, (byte) 40);

            ByteData byteData = new ByteData();
            byteData.setId(1L);
            byteData.setDataLength(100);
            byteData.setData(bytes);

            rwf.write(byteData);

            ByteData read = rwf.read();
            assertNotNull(read);
            assertEquals(1L, read.getId());
            assertEquals(100, read.getDataLength());
        }
    }

    @Test
    @SneakyThrows
    public void testWriteAndGetManyTime() {
        Path logFile = Paths.get("logs/cache/index-1.log");
        Files.createFile(logFile);

        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 1);
        try (ReadWriteFile rwf = new ReadWriteFile(logFile, indexFile, 1 << 20, 1)) {
            byte[] bytes = new byte[128];
            Arrays.fill(bytes, (byte) 40);

            ByteData byteData = new ByteData();
            byteData.setData(bytes);

            for (int i = 1; i < 11; i++) {
                byteData.setId(i);
                byteData.setDataLength(i * 10);
                rwf.write(byteData);
            }

            for (int i = 1; i < 11; i++) {
                ByteData read = rwf.read();
                assertNotNull(read);
                assertEquals(i, read.getId());
                assertEquals(i * 10, read.getDataLength());
            }

            ByteData read = rwf.read();
            assertNull(read);
        }
    }

    @Test
    @SneakyThrows
    public void testWriteOverSize() {
        Path logFile = Paths.get("logs/cache/index-1.log");
        Files.createFile(logFile);

        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 1);
        try (ReadWriteFile rwf = new ReadWriteFile(logFile, indexFile, 8192, 1)) {
            byte[] bytes = new byte[128];
            Arrays.fill(bytes, (byte) 40);

            ByteData byteData = new ByteData();
            byteData.setData(bytes);
            byteData.setDataLength(100);

            for (int i = 1; i < 10; i++) {
                byteData.setId(i);
                boolean success = rwf.write(byteData);
                assertTrue(success);
            }

            boolean success = rwf.write(byteData);
            assertFalse(success);

            ByteData read = rwf.read();
            assertNotNull(read);
            assertEquals(1L, read.getId());

            assertTrue(rwf.write(byteData));
        }
    }


    @Test
    @SneakyThrows
    public void testReadOverSize() {
        Path logFile = Paths.get("logs/cache/index-1.log");
        Files.createFile(logFile);

        IndexFile indexFile = new IndexFile(Paths.get("logs/cache"), 1);
        try (ReadWriteFile rwf = new ReadWriteFile(logFile, indexFile, 8192, 1)) {
            byte[] bytes = new byte[128];
            Arrays.fill(bytes, (byte) 40);

            ByteData byteData = new ByteData();
            byteData.setDataLength(100);
            byteData.setData(bytes);

            for (int i = 1; i < 10; i++) {
                byteData.setId(i);
                rwf.write(byteData);
            }

            for (int i = 0; i < 10; i++) {
                rwf.read();
            }

            for (int i = 1; i < 10; i++) {
                byteData.setId(i);
                boolean write = rwf.write(byteData);
                assertTrue(write);
            }

            for (int i = 0; i < 10; i++) {
                ByteData read = rwf.read();
                assertNotNull(read);
            }
        }
    }


}
