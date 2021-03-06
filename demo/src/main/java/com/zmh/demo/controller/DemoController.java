package com.zmh.demo.controller;

import com.google.common.util.concurrent.RateLimiter;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.utils.Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadFactory;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getNowTime;
import static java.lang.System.currentTimeMillis;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.nonNull;

@RestController
@Slf4j
public class DemoController {

    private final static ThreadFactory threadFactory = ThreadUtils.namedDaemonThreadFactory("DemoController");

    private final static int permits = 1_0000;

    private static volatile RateLimiter limiter = RateLimiter.create(permits);

    private static String[] text = new String[100];

    static {
        for (int i = 0; i < 100; i++) {
            text[i] = Utils.getText(120);
        }
    }

    @GetMapping("test")
    public void test() {
        debugLog("begin:" + getNowTime());
        for (int i = 10; i > 0; i--) {
            log.info(Utils.getText(i));
        }
        debugLog("end:" + getNowTime());
    }

    @GetMapping("/testLog/{diverse}/{threadCount}/{seconds}/{qps}")
    @SneakyThrows
    public void testLog(@PathVariable("diverse") int diverse, @PathVariable("threadCount") int threadCount, @PathVariable("seconds") int seconds, @PathVariable("qps") int qps) {
        debugLog("===========================================begin:" + getNowTime());

        long start = currentTimeMillis();

        int total = seconds * qps;
        int count = seconds * permits / threadCount;

        List<Thread> list = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Thread thread = threadFactory.newThread(() -> {
                int index = 0;
                for (int j = 0; j < count; j++) {
                    limiter.acquire();
                    for (int k = 0; k < qps / permits; k++) {
                        log.info(text[index++ % diverse]);
                    }
                }
            });
            list.add(thread);
            thread.start();
        }

        for (int i = 0; i < list.size(); i++) {
            list.get(i).join();
        }

        long time =  currentTimeMillis() - start;
        debugLog("===========================================???????????????" + time + " " + new BigDecimal( total / 10).divide(new BigDecimal(time), 2, HALF_UP) + "w/QPS");

        debugLog("===========================================end:" + getNowTime());
    }

    @GetMapping("/testKafka")
    public void testKafka() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "10.106.112.59:9092");//???????????????kafka????????????????????????list??????????????????????????????hosts?????????????????????servers????????????host1:port1,host2:port2,???????????????????????????????????????????????????down??????
        configs.put("compression.type", "lz4");//?????????????????????none???Producer?????????????????????????????????????????????none, gzip, snappy, or lz4
        configs.put("batch.size", 4096);
        configs.put("max.block.ms", 1);//long????????????60000?????????block???????????????buffer??????????????????metadata???????????????block

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());

        String text = Utils.getText(1);
        debugLog("begin:" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss SSS"));
        for (int i = 0; i < 100_0000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("log2", text + i);
            producer.send(record, (metadata, e) -> {
                if (nonNull(e)) {
                    debugLog("msg send error:" + e.getMessage());
                } else {
                    logTime();
                }
            });
        }
        debugLog("end:" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss SSS"));
        ThreadUtils.sleep(100_000);
    }

    private int logIndex;
    private int interval;

    private void logTime() {
        logIndex++;
        if (logIndex > 99_9900) {
            interval++;
            if (interval == 10) {
                interval = 0;
                debugLog(logIndex + ":" + DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss SSS"));
            }
        }
    }

}
