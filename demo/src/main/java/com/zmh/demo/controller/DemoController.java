package com.zmh.demo.controller;

import com.google.common.util.concurrent.RateLimiter;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.ThreadUtils;
import com.zmh.fastlog.worker.file.FIFOQueue;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import static java.util.concurrent.TimeUnit.SECONDS;

@RestController
@Slf4j
public class DemoController {

    private final static ThreadFactory threadFactory = ThreadUtils.namedDaemonThreadFactory("DemoController");

    private final static int permits = 1_0000;

    private static volatile RateLimiter limiter = RateLimiter.create(permits);

    private static String[] text = new String[100];

    static {
        for (int i = 0; i < 100; i++) {
            text[i] = getText1(120);
        }
    }

    @GetMapping("test")
    public void test() {
        debugLog("begin:" + getNowTime());
        for (int i = 10; i > 0; i--) {
            log.info(getText1(i));
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
        debugLog("===========================================实际耗时：" + time + " " + new BigDecimal( total / 10).divide(new BigDecimal(time), 2, HALF_UP) + "w/QPS");

        debugLog("===========================================end:" + getNowTime());
    }

    @GetMapping("/testKafka")
    public void testKafka() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "10.106.112.59:9092");//用于建立与kafka集群的连接，这个list仅仅影响用于初始化的hosts，来发现全部的servers。格式：host1:port1,host2:port2,…，数量尽量不止一个，以防其中一个down了。
        configs.put("compression.type", "lz4");//字符串，默认值none。Producer用于压缩数据的压缩类型，取值：none, gzip, snappy, or lz4
        configs.put("batch.size", 4096);
        configs.put("max.block.ms", 1);//long，默认值60000。控制block的时长，当buffer空间不够或者metadata丢失时产生block

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());

        String text = getText1(1);
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



    private static String getText1(int size) {
        List<String> stringList = getText(size);
        Collections.shuffle(stringList);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stringList.size(); i++) {
            sb.append(stringList.get(i));
        }
        return sb.toString();
    }

    private static List<String> getText(int size) {
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
    private static String getRandomChar() {
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
