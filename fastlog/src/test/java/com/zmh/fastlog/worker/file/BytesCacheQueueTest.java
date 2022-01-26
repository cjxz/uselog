package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

public class BytesCacheQueueTest extends BeforeDeleteFile {

    @Test
    public void testBytesCacheQueue() {
        BytesCacheQueue queue = new BytesCacheQueue(1024);
        long seq = 0L;

        ByteData byteData = getByteData(100);
        for (int i = 0; i < 5; i++) {
            byteData.setId(seq++);
            queue.put(byteData);
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
        long seq = 0L;
        ByteData byteData = getByteData(100);

        boolean success;

        //正常放7个
        for (int i = 0; i < 7; i++) {
            byteData.setId(seq++);
            success = queue.put(byteData);
            assertTrue(success);
        }

        //再放一个满了
        success = queue.put(byteData);
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

        ByteData byteData = getByteData(100);
        for (int i = 0; i < 8; i++) {
            byteData.setId(i);
            tail.put(byteData);
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

    @SuppressWarnings("SameParameterValue")
    private ByteData getByteData(int length) {
        byte[] array = new byte[128];
        Arrays.fill(array, (byte) 50);

        return new ByteData(1, array, length);
    }

}
