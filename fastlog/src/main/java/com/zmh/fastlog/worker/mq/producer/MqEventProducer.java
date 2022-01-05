package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;

public interface MqEventProducer {

    boolean connect();

    void sendEvent(ByteDisruptorEvent event);

    void flush();

    void close();
}
