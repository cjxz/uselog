package com.zmh.ringbuffer.demo;

import com.google.common.util.concurrent.RateLimiter;
import com.zmh.ringbuffer.demo.ring.Ring1;
import lombok.SneakyThrows;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestRing {

    @Test
    @SneakyThrows
    public void test() {
        RateLimiter limiter = RateLimiter.create(5_0000);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

        System.out.println("begin:" + sdf.format(new Date()));

        Ring1 ring1 = new Ring1();
        for (int i = 0; i < 100_0000; i++) {
            ring1.sendMsg();
            limiter.acquire();
        }

        System.out.println("end:" + sdf.format(new Date()));

        Thread.sleep(2_000);
    }
}
