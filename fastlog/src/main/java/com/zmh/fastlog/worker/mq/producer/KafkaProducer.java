package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import com.zmh.fastlog.model.event.ByteEvent;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static com.zmh.fastlog.utils.Utils.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class KafkaProducer implements MqProducer {

    private org.apache.kafka.clients.producer.KafkaProducer<String, ByteBuffer> producer;

    private final String url;
    private final String topic;

    private final int batchMessageSize;

    private long totalMissingCount = 0;
    private int kafkaMissingCount = 0;

    public KafkaProducer(String url, String topic, int batchMessageSize) {
        if (isNotBlank(topic)) {
            topic = topic.toLowerCase();
        }
        this.url = url;
        this.topic = topic;
        this.batchMessageSize = batchMessageSize;
    }

    @Override
    public boolean connect() {
        if (isBlank(url) || isBlank(topic)) {
            return false;
        }

        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", url);//用于建立与kafka集群的连接，这个list仅仅影响用于初始化的hosts，来发现全部的servers。格式：host1:port1,host2:port2,…，数量尽量不止一个，以防其中一个down了。
        configs.put("compression.type", "lz4");//字符串，默认值none。Producer用于压缩数据的压缩类型，取值：none, gzip, snappy, or lz4
        configs.put("batch.size", batchMessageSize);
        configs.put("max.block.ms", 0);//long，默认值60000。控制block的时长，当buffer空间不够或者metadata丢失时产生block
        //configs.put("buffer.memory", );// long, 默认值33554432。 Producer可以用来缓存数据的内存大小。todo zmh

        try {
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(configs, new StringSerializer(), new ByteBufferSerializer());
        } catch (Exception ignored) {
        }
        return nonNull(producer);
    }

    @Override
    public void sendEvent(ByteDisruptorEvent event) {
        ByteEvent byteEvent = event.getByteEvent();
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, byteEvent.getBuffer());

        producer.send(record, (metadata, e) -> {
            if (nonNull(e)) {
                debugLog("fastlog kafka sendEvent fail, e:" + e.getMessage());
                kafkaMissingCount++;
            } else {
                event.clear();
            }
        });
    }

    public boolean hasMissedMsg() {
        boolean result = kafkaMissingCount > 0;
        if (result) {
            totalMissingCount += kafkaMissingCount;
            debugLog("fastlog kafka mission count:" + kafkaMissingCount + ", total:" + totalMissingCount);
            kafkaMissingCount = 0;
        }
        return result;
    }

    @Override
    @SneakyThrows
    public boolean heartbeat() {
        if (isNull(producer)) {
            return false;
        }

        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, ByteBuffer.wrap("heartbeat".getBytes()));
        Future<RecordMetadata> send = producer.send(record);
        try {
            RecordMetadata recordMetadata = send.get();
            return nonNull(recordMetadata);
        } catch (Exception e) {
            debugLog("fastlog kafka heartbeat fail, e:" + e.getMessage());
            return false;
        }
    }

    @Override
    public void flush() {
        try {
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (nonNull(producer)) {
            sneakyInvoke(producer::flush);
            safeClose(producer);
        }
    }
}
