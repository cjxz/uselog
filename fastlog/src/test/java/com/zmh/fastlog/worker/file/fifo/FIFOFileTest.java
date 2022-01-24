package com.zmh.fastlog.worker.file.fifo;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Objects.nonNull;
import static org.junit.Assert.*;

public class FIFOFileTest extends BeforeDeleteFile {

    @Test
    public void testFIFOFilePutAndGetSingleFile() {
        try (FIFOFile fifoFile = new FIFOFile("logs/cache",1024, 100)) {
            for (int i = 1; i < 10; i++) {
                byte[] array = new byte[128];
                Arrays.fill(array, (byte) 40);

                ByteData byteEvent = new ByteData(i, array, i * 10);
                fifoFile.put(byteEvent);
            }

            assertEquals(1, fifoFile.getFileNum());
            assertEquals(558, fifoFile.getFileSize());

            for (int i = 1; i < 10; i++) {
                ByteData message = fifoFile.get();
                assertNotNull(message);
                assertEquals(i, message.getId());
                assertEquals(i * 10, message.getDataLength());
                fifoFile.next();
            }

            ByteData message = fifoFile.get();
            assertNull(message);
        }
    }

    @Test
    public void testFIFOFilePutManyFile() {
        try (FIFOFile fifoFile = new FIFOFile("logs/cache",32 * 1024, 100)) {
            for (int i = 1; i < 10240; i++) {
                byte[] array = new byte[128];
                Arrays.fill(array, (byte) 40);

                ByteData byteEvent = new ByteData(i, array, (i % 10) * 11);
                fifoFile.put(byteEvent);
            }

        }
    }

    @Test
    public void testFIFOFileQueuePutAndGetNum() {
        try (FIFOFile fifoFile = new FIFOFile("logs/cache", 32 * 1024 * 1024, 100)) {
            long seq = 0L;

            String text = "中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123";

            byte[] bytes = Arrays.copyOf(text.getBytes(), 200);
            ByteData byteData = new ByteData(0, bytes, 192);

            ByteData message;

            int read = 0;
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10240; j++) {
                    byteData.setId(seq++);
                    fifoFile.put(byteData);
                }

                int num = RandomUtils.nextInt(100, 10240), count = 0;
                System.out.println(i + ":" + num);
                while (count++ < num && nonNull(message = fifoFile.get())) {
                    read++;
                    fifoFile.next();
                }
            }

            ThreadUtils.sleep(1000);

            int size = 0;
            while (nonNull(message = fifoFile.get())) {
                size++;
                fifoFile.next();
            }
            assertEquals(100 * 1_0000 - read, size);
        }
    }

}
