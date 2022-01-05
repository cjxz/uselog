package com.zmh.fastlog.worker.mq.producer;

import com.zmh.fastlog.model.event.ByteDisruptorEvent;

public interface MqProducer extends AutoCloseable {

    boolean connect();

    void sendEvent(ByteDisruptorEvent event);

    boolean hasMissedMsg();

    boolean heartbeat();

    void flush();
}
