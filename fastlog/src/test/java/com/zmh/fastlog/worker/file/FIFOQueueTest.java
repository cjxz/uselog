package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Objects.nonNull;
import static org.junit.Assert.*;

public class FIFOQueueTest extends BeforeDeleteFile {

    @Test
    public void testFIFOFileQueuePutAndGet() {
        try (FIFOQueue fifoFile = new FIFOQueue("logs/cache",1024, 100)) {
            long seq = 1L;
            ByteData byteData = getByteData(100);

            for (int i = 0; i < 7; i++) {
                byteData.setId(seq++);
                fifoFile.put(byteData);
            }

            ByteData message = fifoFile.get();
            assertNotNull(message);
        }
    }

    @Test
    public void testFIFOFileQueuePutAndGetNum() {
        try (FIFOQueue fifoFile = new FIFOQueue("logs/cache", 32 * 1024 * 1024, 100)) {
            long seq = 0L;

            ByteData byteData = getByteData(100);

            int read = 0;
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 1_0000; j++) {
                    byteData.setId(seq++);
                    fifoFile.put(byteData);
                }

                int num = RandomUtils.nextInt(100, 1_0000), count = 0;
                while (count++ < num && nonNull(fifoFile.get())) {
                    read++;
                    fifoFile.next();
                }
            }

            ThreadUtils.sleep(1000);

            int size = 0;
            while (nonNull(fifoFile.get())) {
                size++;
                fifoFile.next();
            }
            assertEquals(100 * 1_0000 - read, size);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private ByteData getByteData(int length) {
        byte[] array = new byte[128];
        Arrays.fill(array, (byte) 50);

        return new ByteData(1L, array, length);
    }
}
