package com.zmh.fastlog;

import org.junit.Test;

import java.util.Calendar;

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
}

