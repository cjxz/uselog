package com.zmh.ringbuffer.demo.controller;

import com.zmh.ringbuffer.demo.ring.Ring1;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@Slf4j
public class DemoController {

    @GetMapping("test")
    public void test() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

        System.out.println("begin:" + sdf.format(new Date()));

        Ring1 ring1 = new Ring1();
        for (int i = 0; i < 100_0000; i++) {
            ring1.sendMsg();
        }

        System.out.println("end:" + sdf.format(new Date()));
    }
}
