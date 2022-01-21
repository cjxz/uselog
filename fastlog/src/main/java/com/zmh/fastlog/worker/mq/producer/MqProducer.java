package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.EventSlot;

public interface MqProducer extends AutoCloseable {

    void connect();

    void sendEvent(EventSlot event);

    boolean isReady();

    void flush();
}
