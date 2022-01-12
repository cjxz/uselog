package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.FileMqMessage;
import com.zmh.fastlog.model.event.ByteEvent;
import com.zmh.fastlog.utils.DateSequence;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.junit.Assert.*;

public class FileWorkerTest {

    @Ignore
    @Test
    public void test() {
        try (FIFOQueue fifo = new FIFOQueue(64 * 1024 * 1024, 100, "logs/cache")) {
            StopWatch watch = new StopWatch();
            DateSequence seq = new DateSequence();

            byte[] bytes = getText(200).getBytes();

            watch.start();
            for (int i = 0; i < 10000; i++) {
                for (int j = 0; j < 1000; j++) {
                    ByteEvent byteEvent = new ByteEvent();
                    byteEvent.setBuffer(ByteBuffer.wrap(bytes));
                    byteEvent.setId(seq.next());
                    fifo.put(byteEvent);
                }

                for (int j = 0; j < 10; j++) {
                    fifo.get();
                }
            }
            watch.stop();
            System.out.println(watch.formatTime());
            System.out.println(1000 / watch.getTime(TimeUnit.SECONDS));
        }
    }

    @Before
    public void after() {
        deleteFile(new File("logs/cache"));
    }

    private String getText(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(getRandomChar());
        }
        sb.append("。");
        return sb.toString();
    }

    //随机生成常见汉字
    @SneakyThrows
    private String getRandomChar() {
        int highCode;
        int lowCode;

        Random random = new Random();

        highCode = (176 + Math.abs(random.nextInt(39))); //B0 + 0~39(16~55) 一级汉字所占区
        lowCode = (161 + Math.abs(random.nextInt(93))); //A1 + 0~93 每区有94个汉字

        byte[] b = new byte[2];
        b[0] = (Integer.valueOf(highCode)).byteValue();
        b[1] = (Integer.valueOf(lowCode)).byteValue();
        return new String(b, "GBK");
    }

    @Test
    public void testFIFOFileQueuePutAndGet() {
        try (FIFOQueue fifoFile = new FIFOQueue(1024, 100,"logs/cache")) {
            DateSequence seq = new DateSequence();

            for (int i = 0; i < 7; i++) {
                putToFile(128, fifoFile, (byte) (50 + i), seq.next());
            }

            AbstractMqMessage message = fifoFile.get();
            assertNotNull(message);
        }
    }

    @Test
    public void testFIFOFileQueuePutAndGetNum() {
        try (FIFOQueue fifoFile = new FIFOQueue(32 * 1024 * 1024, 100,"logs/cache")) {
            DateSequence seq = new DateSequence();

            String text = "中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123中文English123";
            byte[] bytes = text.getBytes();
            AbstractMqMessage message;

            int read = 0;
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 1_0000; j++) {
                    ByteEvent byteEvent = new ByteEvent();

                    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
                    buffer.put(bytes);
                    byteEvent.setBuffer(buffer);
                    byteEvent.setId(seq.next());
                    fifoFile.put(byteEvent);
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteFile(File file) {
        //判断文件不为null或文件目录存在
        if (isNull(file) || !file.exists()) {
            return;
        }

        //取得这个目录下的所有子文件对象
        File[] files = file.listFiles();
        if (isNull(files)) {
            return;
        }

        //遍历该目录下的文件对象
        for (File f : files) {
            //打印文件名
            String name = f.getName();
            System.out.println("delete:" + name);
            //判断子目录是否存在子目录,如果是文件则删除
            if (f.isDirectory()) {
                deleteFile(f);
            } else {
                f.delete();
            }
        }
    }

    @Test
    public void testBytesCacheQueue() {
        BytesCacheQueue queue = new BytesCacheQueue(1024);
        DateSequence seq = new DateSequence();

        for (int i = 0; i < 5; i++) {
            putToQueue(queue, (byte) (50 + i), seq.next());
        }

        assertFalse(queue.isEmpty());
        assertEquals(5 * (100 + 4 + 8), queue.getBytes().writerIndex());

        for (int i = 0; i < 5; i++) {
            FileMqMessage message = queue.get();
            assertNotNull(message);
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testBytesCacheQueueFull() {
        BytesCacheQueue queue = new BytesCacheQueue(800);

        DateSequence seq = new DateSequence();

        boolean success = false;
        //正常放7个
        for (int i = 0; i < 7; i++) {
            success = putToQueue(queue, (byte) (50 + i), seq.next());
        }
        assertTrue(success);

        //再放一个满了
        success = putToQueue(queue, (byte) 57, seq.next());
        assertFalse(success);
        assertFalse(queue.isEmpty());

        //取7个
        for (int i = 0; i < 7; i++) {
            FileMqMessage message = queue.get();
            assertNotNull(message);
        }
        assertTrue(queue.isEmpty());

        //再取一个取不到，队列重置，又可以继续从头开始放了
        FileMqMessage message = queue.get();
        assertNull(message);
        assertEquals(0, queue.getBytes().readerIndex());
        assertEquals(0, queue.getBytes().writerIndex());
    }

    @Test
    public void testBytesCacheQueueCopyTo() {
        BytesCacheQueue tail = new BytesCacheQueue(800);
        BytesCacheQueue head = new BytesCacheQueue(800);
        DateSequence seq = new DateSequence();


        for (int i = 0; i < 8; i++) {
            putToQueue(tail, (byte) (50 + i), seq.next());
        }

        tail.copyTo(head);
        assertFalse(head.isEmpty());

        for (int i = 0; i < 7; i++) {
            FileMqMessage message = head.get();
            assertNotNull(message);
        }
        assertTrue(head.isEmpty());

        FileMqMessage message = head.get();
        assertNull(message);
    }


    private boolean putToQueue(BytesCacheQueue queue, byte b, long id) {
        byte[] array = new byte[128];
        Arrays.fill(array, b);

        ByteEvent byteEvent = new ByteEvent();

        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.put(array, 0, 100);
        byteEvent.setBuffer(buffer);
        byteEvent.setId(id);
        return queue.put(byteEvent);
    }

    private void putToFile(int byteSize, FIFOQueue file, byte b, long id) {
        byte[] array = new byte[128];
        Arrays.fill(array, b);

        ByteEvent byteEvent = new ByteEvent();

        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.put(array, 0, 100);
        byteEvent.setBuffer(buffer);
        byteEvent.setId(id);
        file.put(byteEvent);
    }
}
