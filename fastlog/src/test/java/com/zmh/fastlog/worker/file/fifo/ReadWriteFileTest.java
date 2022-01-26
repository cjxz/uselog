package com.zmh.fastlog.worker.file.fifo;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createWriteFile;
import static org.junit.Assert.*;

public class ReadWriteFileTest extends BeforeDeleteFile {

    private Path logFile;
    private IndexFile indexFile;

    @Before
    @SneakyThrows
    public void before() {
        Path path = Paths.get(FOLDER);
        Files.createDirectories(path);
        this.indexFile = new IndexFile(path);

        this.logFile = Paths.get(FOLDER + "/queue.log");
        Files.createFile(logFile);
    }

    @Test
    @SneakyThrows
    public void testWriteAndGetSingleTime() {
        try (ReadWriteFile rwf = createWriteFile(logFile, indexFile, 1 << 20)) {
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
        try (ReadWriteFile rwf = createWriteFile(logFile, indexFile, 1 << 20)) {
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
        try (ReadWriteFile rwf = createWriteFile(logFile, indexFile, 1024)) {
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
        try (ReadWriteFile rwf = createWriteFile(logFile, indexFile, 1024)) {
            byte[] bytes = new byte[128];
            Arrays.fill(bytes, (byte) 40);

            ByteData byteData = new ByteData();
            byteData.setDataLength(100);
            byteData.setData(bytes);

            for (int i = 1; i < 10; i++) {
                byteData.setId(i);
                rwf.write(byteData);
            }

            for (int i = 1; i < 10; i++) {
                rwf.read();
            }

            for (int i = 1; i < 10; i++) {
                byteData.setId(i);
                byteData.setDataLength(i * 10);
                boolean write = rwf.write(byteData);
                assertTrue(write);
            }

            for (int i = 1; i < 10; i++) {
                ByteData read = rwf.read();
                assertNotNull(read);
                assertEquals(i, read.getId());
                assertEquals(i * 10, read.getDataLength());
            }
        }
    }


}
