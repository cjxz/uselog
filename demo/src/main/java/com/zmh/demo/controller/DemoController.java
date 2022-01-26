package com.zmh.demo.controller;

import com.google.common.util.concurrent.RateLimiter;
import com.zmh.fastlog.utils.ThreadUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getNowTime;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.nonNull;

@RestController
@Slf4j
public class DemoController {
    private RateLimiter limiter = RateLimiter.create(10000);

    @GetMapping("test")
    public void test() {
        debugLog("begin:" + getNowTime());
        for (int i = 10; i > 0; i--) {
            log.info(getText(i));
        }
        debugLog("end:" + getNowTime());
    }

    private ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

    @GetMapping("testLog")
    @SneakyThrows
    public void testLog() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String[] text = new String[100];
        for (int i = 0; i < 100; i++) {
            text[i] = getText(100);
        }

        CountDownLatch taskLatch = new CountDownLatch(100_0000);
        for (int i = 0; i < 1_0000; i++) {
            limiter.acquire();

            for (int j = 0; j < 100; j++) {
                int index = j;
                pool.execute(() -> {
                    log.info(text[index]);
                    taskLatch.countDown();
                });
            }
        }
        //当前线程阻塞，等待计数器置为0
        taskLatch.await();

        stopWatch.stop();
        long time = stopWatch.getTime(TimeUnit.MILLISECONDS);
        debugLog("耗时：" + time + " " + new BigDecimal(100000).divide(new BigDecimal(time), 2, HALF_UP) + "w/QPS");
    }

    @GetMapping("/testKafka")
    public void testKafka() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "10.106.112.59:9092");//用于建立与kafka集群的连接，这个list仅仅影响用于初始化的hosts，来发现全部的servers。格式：host1:port1,host2:port2,…，数量尽量不止一个，以防其中一个down了。
        configs.put("compression.type", "lz4");//字符串，默认值none。Producer用于压缩数据的压缩类型，取值：none, gzip, snappy, or lz4
        configs.put("batch.size", 4096);
        configs.put("max.block.ms", 1);//long，默认值60000。控制block的时长，当buffer空间不够或者metadata丢失时产生block

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());

        String text = getText(1);
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
