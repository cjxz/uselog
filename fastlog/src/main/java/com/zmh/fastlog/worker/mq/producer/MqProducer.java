package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;

public interface MqProducer extends AutoCloseable {

    void connect();

    void sendEvent(ByteDisruptorEvent event);

    boolean isReady();

    void flush();
}
