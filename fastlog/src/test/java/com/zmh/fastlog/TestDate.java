package com.zmh.fastlog;

import io.appulse.utils.Bytes;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.shade.io.airlift.compress.lz4.Lz4Compressor;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static com.zmh.fastlog.utils.Utils.debugLogCondition;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createReadFile;

public class TestDate {

    @Test
    public void test() {
        Calendar calendar = new Calendar.Builder().build();
        calendar.setTimeInMillis(1643828583000L);

        int year = calendar.get(Calendar.YEAR);

        int month = calendar.get(Calendar.MONTH) + 1;

        int day = calendar.get(Calendar.DAY_OF_MONTH);

        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        int minute = calendar.get(Calendar.MINUTE);

        int seconds = calendar.get(Calendar.SECOND);

        System.out.println(String.format("%d,%d,%d,%d,%d,%d", year, month, day, hour, minute, seconds));
    }

    @Test
    public void test1() {
        System.out.println(getText1(300));
        System.out.println(getText1(300));
        System.out.println(getText1(300));
        System.out.println(getText1(300));

    }

    private String getText1(int size) {
        List<String> stringList = getText(size);
        Collections.shuffle(stringList);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stringList.size(); i++) {
            sb.append(stringList.get(i));
        }
        return sb.toString();
    }

    private List<String> getText(int size) {
        List<String> list = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < size / 10 - 10; j++) {
                list.add(RandomStringUtils.randomPrint(1));
            }
            for (int j = 0; j < 10; j++) {
                list.add(getRandomChar());
            }
        }
        return list;
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

