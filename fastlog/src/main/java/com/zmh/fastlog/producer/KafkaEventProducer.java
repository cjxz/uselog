package com.zmh.fastlog.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.zmh.fastlog.utils.Utils.safeClose;
import static com.zmh.fastlog.utils.Utils.sneakyInvoke;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class KafkaEventProducer implements MqEventProducer {

    private KafkaProducer<String, ByteBuffer> producer;

    private final String url;
    private final String topic;

    private final int batchMessageSize;

    public KafkaEventProducer(String url, String topic, int batchMessageSize) {
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
            producer = new KafkaProducer<>(configs, new StringSerializer(), new ByteBufferSerializer());
        } catch (Exception ignored) {
        }
        return nonNull(producer);
    }

    @Override
    public void sendEvent(MqEvent event) {
        ByteEvent byteEvent = event.getByteEvent();
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(topic, byteEvent.getBuffer());

        producer.send(record, (metadata, e) -> {
            if (nonNull(e)) {
                e.printStackTrace();
            } else {
                event.clear();
            }
        });
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
