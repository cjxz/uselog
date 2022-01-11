package com.zmh.fastlog;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;

import java.util.Date;

public class TestDate {

    @Test
    @SneakyThrows
    public void test() {

        System.out.println(DateFormatUtils.format(new Date(0x000001ffffffffffL + 1546300800000L), "yyyy-MM-dd HH:mm:ss"));
    }
}
