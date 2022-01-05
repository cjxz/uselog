package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;

import static com.zmh.fastlog.utils.Utils.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.api.CompressionType.ZLIB;
import static org.apache.pulsar.client.api.Schema.BYTES;
import static org.apache.pulsar.shade.org.apache.commons.lang.StringUtils.isBlank;

public class PulsarEventProducer implements MqEventProducer {

    private final String url;
    private final String topic;
    private final int batchMessageSize;


    private volatile PulsarClient client;
    private volatile Producer<byte[]> producer;

    public PulsarEventProducer(String url, String topic, int batchMessageSize) {
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

        try {
            if (isNull(client)) {
                client = PulsarClient.builder()
                        .serviceUrl(url)
                        .enableTcpNoDelay(true)
                        .build();
            }
            if (nonNull(client) && isNull(producer)) {
                producer = client.newProducer(BYTES)
                        .topic(topic)
                        .batchingMaxMessages(batchMessageSize) // 测试下来每秒大约能有20几个批次的提交, 乘以每批大小就可以得到吞吐量
                        .batchingMaxPublishDelay(5, MILLISECONDS) // 每批的时间大约50ms, 延迟为50/5
                        .enableBatching(true)
                        .blockIfQueueFull(true)
                        .maxPendingMessages(batchMessageSize << 1)
                        .sendTimeout(30, SECONDS)
                        .compressionType(ZLIB)
                        .create();
            }
        } catch (PulsarClientException e) {
            debugLog(e.getMessage());
        }
        if (nonNull(client) && nonNull(producer)) {
            debugLog("pulsar connected![" + url + "][" + topic + "]");
            return true;
        }
        return false;
    }

    @Override
    public void sendEvent(ByteDisruptorEvent event) {
        TypedMessageBuilderImpl<byte[]> pulsarMessage = (TypedMessageBuilderImpl<byte[]>) producer.newMessage();

        int eventSize = event.getByteEvent().getBufferLen();

        pulsarMessage.value(event.getByteEvent().getBuffer().array());
        pulsarMessage.getContent().limit(eventSize);

        pulsarMessage.sendAsync()
                .exceptionally(err -> {
                    err.printStackTrace();
                    return null;
                })
                .thenRun(event::clear);
    }

    @Override
    public void flush() {
        try {
            producer.flush();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (nonNull(producer)) {
            sneakyInvoke(producer::flush);
            safeClose(producer);
        }
        safeClose(client);
    }
}
