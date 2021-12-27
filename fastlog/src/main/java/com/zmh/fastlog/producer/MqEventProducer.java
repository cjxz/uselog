package com.zmh.fastlog.producer;

public interface MqEventProducer {

    boolean connect();

    void sendEvent(MqEvent event);

    void flush();

    void close();
}
