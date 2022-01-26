package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Objects.nonNull;
import static org.junit.Assert.*;

public class FileWorkerTest extends BeforeDeleteFile {

    @Test
    public void testFIFOFileQueuePutAndGet() {
        try (FIFOQueue fifoFile = new FIFOQueue("logs/cache",1024, 100)) {
            long seq = 1L;

            for (int i = 0; i < 7; i++) {
                putToFile(128, fifoFile, (byte) (50 + i), seq++);
            }

            ByteData message = fifoFile.get();
            assertNotNull(message);
        }
    }

    @Test
    public void testFIFOFileQueuePutAndGetNum() {
        try (FIFOQueue fifoFile = new FIFOQueue("logs/cache", 32 * 1024 * 1024, 100)) {
            long seq = 1L;

            String text = "中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123";
            byte[] bytes = text.getBytes();
            ByteData message;

            int read = 0;
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 1_0000; j++) {
                    ByteData byteData = new ByteData(seq++, bytes, bytes.length);
                    fifoFile.put(byteData);
                }

                int num = RandomUtils.nextInt(100, 10000), count = 0;
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

    @Test
    public void testBytesCacheQueue() {
        BytesCacheQueue queue = new BytesCacheQueue(1024);
        long seq = 1L;

        for (int i = 0; i < 5; i++) {
            putToQueue(queue, (byte) (50 + i), seq++);
        }

        assertFalse(queue.isEmpty());
        assertEquals(5 * (100 + 4 + 8), queue.getBytes().writerIndex());

        for (int i = 0; i < 5; i++) {
            ByteData message = queue.get();
            assertNotNull(message);
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testBytesCacheQueueFull() {
        BytesCacheQueue queue = new BytesCacheQueue(800);

        long seq = 1L;

        boolean success = false;
        //正常放7个
        for (int i = 0; i < 7; i++) {
            success = putToQueue(queue, (byte) (50 + i), seq++);
        }
        assertTrue(success);

        //再放一个满了
        success = putToQueue(queue, (byte) 57, seq++);
        assertFalse(success);
        assertFalse(queue.isEmpty());

        //取7个
        for (int i = 0; i < 7; i++) {
            ByteData message = queue.get();
            assertNotNull(message);
        }
        assertTrue(queue.isEmpty());

        //再取一个取不到，队列重置，又可以继续从头开始放了
        ByteData message = queue.get();
        assertNull(message);
        assertEquals(0, queue.getBytes().readerIndex());
        assertEquals(0, queue.getBytes().writerIndex());
    }

    @Test
    public void testBytesCacheQueueCopyTo() {
        BytesCacheQueue tail = new BytesCacheQueue(800);
        BytesCacheQueue head = new BytesCacheQueue(800);


        for (int i = 0; i < 8; i++) {
            putToQueue(tail, (byte) (50 + i), i);
        }

        tail.copyTo(head);
        assertFalse(head.isEmpty());

        for (int i = 0; i < 7; i++) {
            ByteData message = head.get();
            assertNotNull(message);
        }
        assertTrue(head.isEmpty());

        ByteData message = head.get();
        assertNull(message);
    }


    private boolean putToQueue(BytesCacheQueue queue, byte b, long id) {
        byte[] array = new byte[128];
        Arrays.fill(array, b);

        ByteData byteEvent = new ByteData(id, array, 100);
        return queue.put(byteEvent);
    }

    private void putToFile(int byteSize, FIFOQueue file, byte b, long id) {
        byte[] array = new byte[128];
        Arrays.fill(array, b);

        ByteData byteEvent = new ByteData(id, array, 100);
        file.put(byteEvent);
    }
}
