package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDataSoftRef;

public interface MqProducer extends AutoCloseable {

    void connect();

    void sendEvent(ByteDataSoftRef event);

    boolean isReady();

    void flush();
}
