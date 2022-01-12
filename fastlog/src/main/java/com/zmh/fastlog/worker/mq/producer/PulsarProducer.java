package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledFuture;

import static com.zmh.fastlog.utils.ScheduleUtils.scheduleWithFixedDelay;
import static com.zmh.fastlog.utils.Utils.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.api.CompressionType.ZLIB;
import static org.apache.pulsar.client.api.Schema.BYTES;

public class PulsarProducer implements MqProducer {

    private final String url;
    private final String topic;
    private final int batchMessageSize;


    private volatile PulsarClient client;
    private volatile Producer<byte[]> producer;

    private boolean isReady = false;

    private int totalMissingCount = 0;
    private int pulsarMissingCount = 0;

    public PulsarProducer(String url, String topic, int batchMessageSize) {
        if (isNotBlank(topic)) {
            topic = topic.toLowerCase();
        }
        this.url = url;
        this.topic = topic;
        this.batchMessageSize = batchMessageSize;
    }

    private ScheduledFuture<?> connectFuture;

    @Override
    public void connect() {
        connectFuture = scheduleWithFixedDelay(this::doConnect, 0, 5, SECONDS);
    }

    private ScheduledFuture<?> heartbeatFuture;

    private void doConnect() {
        if (nonNull(client) && nonNull(producer)) {
            connectFuture.cancel(true);

            heartbeatFuture = scheduleWithFixedDelay(this::heartbeat, 0, 2, SECONDS);
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
        }
    }

    @Override
    public void sendEvent(ByteDisruptorEvent event) {
        TypedMessageBuilderImpl<byte[]> pulsarMessage = (TypedMessageBuilderImpl<byte[]>) producer.newMessage();

        ByteBuffer buffer = event.getByteEvent().getBuffer();
        pulsarMessage.value(buffer.array());
        pulsarMessage.getContent().limit(buffer.position());

        pulsarMessage.sendAsync()
            .exceptionally(t -> {
                addMissingCount(t);
                return null;
            })
            .thenRun(event::clear);
    }

    private void addMissingCount(Throwable t) {
        pulsarMissingCount++;
        if (pulsarMissingCount == 10) { //todo zmh config 这里不确定会不会有并发问题
            totalMissingCount += 10;
            isReady = false;
            debugLog("fastlog pulsar sendEvent fail, e:" + t.getMessage() + ",totalMissingCount:" + totalMissingCount);
        }
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @SneakyThrows
    private void heartbeat() {
        if (isReady) {
            return;
        }

        try {
            if (nonNull(producer.send("heartbeat".getBytes()))) {
                this.isReady = true;
            }
        } catch (Exception e) {
            debugLog("fastlog pulsar heartbeat fail, e:" + e.getMessage());
        }

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
        if (nonNull(heartbeatFuture)) {
            heartbeatFuture.cancel(true);
        }
    }
}
