package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import com.zmh.fastlog.worker.file.fifo.FIFOFile;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FIFOPerformanceTest extends BeforeDeleteFile {

    @Test
    public void test() {
        //try (FIFOFile fifo = new FIFOFile("logs/cache", 64 * 1024 * 1024, 100)) { //直接写磁盘
        try (FIFOQueue fifo = new FIFOQueue("logs/cache", 64 * 1024 * 1024, 100)) { //先写内存，再批量写磁盘
            StopWatch watch = new StopWatch();
            long seq = 1L;

            byte[] bytes = getText(200).getBytes();
            ByteData byteEvent = new ByteData(0, bytes, bytes.length);

            watch.start();
            for (int i = 0; i < 1000; i++) {
                for (int j = 0; j < 10000; j++) {
                    byteEvent.setId(seq++);
                    fifo.put(byteEvent);
                }

                for (int j = 0; j < 1000; j++) {
                    fifo.get();
                }
            }
            watch.stop();
            System.out.println(watch.formatTime());
            System.out.println(1000 / watch.getTime(TimeUnit.SECONDS));
        }
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

}
