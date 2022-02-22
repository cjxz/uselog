package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.EventSlot;
import com.zmh.fastlog.model.message.ByteData;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleWithFixedDelay;
import static com.zmh.fastlog.utils.Utils.*;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class KafkaProducer implements MqProducer {

    private org.apache.kafka.clients.producer.KafkaProducer<String, ByteBuffer> producer;

    private final String url;
    private final String topic;

    private final int batchSize;

    private long totalMissingCount = 0;
    private int kafkaMissingCount = 0;

    private boolean isReady;

    public KafkaProducer(String url, String topic, int batchSize) {
        if (isNotBlank(topic)) {
            topic = topic.toLowerCase();
        }
        this.url = url;
        this.topic = topic;
        this.batchSize = batchSize;
    }

    private ScheduledFuture<?> heartbeatFuture;

    @Override
    public void connect() {
        if (isBlank(url) || isBlank(topic)) {
            return;
        }

        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", url);//用于建立与kafka集群的连接，这个list仅仅影响用于初始化的hosts，来发现全部的servers。格式：host1:port1,host2:port2,…，数量尽量不止一个，以防其中一个down了。
        configs.put("compression.type", "lz4");//字符串，默认值none。Producer用于压缩数据的压缩类型，取值：none, gzip, snappy, or lz4
        configs.put("batch.size", batchSize);
        configs.put("max.block.ms", 200);//long，默认值60000。控制block的时长，当buffer空间不够或者metadata丢失时产生block
        //configs.put("buffer.memory", );// long, 默认值33554432。 Producer可以用来缓存数据的内存大小

        try {
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(configs, new StringSerializer(), new ByteBufferSerializer());
        } catch (Exception ignored) {
        }

        heartbeatFuture = scheduleWithFixedDelay(this::heartbeat, 0, 2, SECONDS);
    }

    //private AtomicInteger count = new AtomicInteger(0);

    @Override
    public void sendEvent(EventSlot event) {
        ByteData byteData = event.getByteData();

        ByteBuffer buffer = ByteBuffer.wrap(byteData.getData());
        buffer.limit(byteData.getDataLength());
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, buffer);

        //int size = byteData.getDataLength();
        //int index = count.addAndGet(size);

        producer.send(record, (metadata, e) -> {
            if (nonNull(e)) {
                addMissingCount(e);
            }
            //count.getAndAdd(-size);
        });

       /* if (index >= 1000) {
            try {
                future.get();
            } catch (Exception e) {
                addMissingCount(e);
            }
        }*/

        event.clear();
    }

    private void addMissingCount(Exception e) {
        kafkaMissingCount++;
        // 当丢失日志数量达到10时，才关闭该生产者，防止偶发的报错
        if (kafkaMissingCount == 10) { //todo zmh config？？是否有并发？？
            totalMissingCount += 10;
            isReady = false;
            debugLog("fastlog kafka sendEvent fail, e:" + e.getMessage() + ",totalMissingCount:" + totalMissingCount);
        }
    }

    public boolean isReady() {
        return isReady;
    }

    @SneakyThrows
    private void heartbeat() {
        if (isReady) {
            return;
        }
        // todo zmh serial
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, ByteBuffer.wrap("heartbeat".getBytes()));
        Future<RecordMetadata> future = producer.send(record);

        try {
            if (nonNull(future.get())) {
                this.isReady = true;
            }
        } catch (Exception e) {
            debugLog("fastlog kafka heartbeat fail, e:" + e.getMessage());
        }
    }

    @Override
    public void flush() {
        //producer.flush();
    }

    @Override
    public void close() {
        if (nonNull(producer)) {
            sneakyInvoke(producer::flush);
            safeClose(producer);
        }
        if (nonNull(heartbeatFuture)) {
            heartbeatFuture.cancel(true);
        }
    }
}
