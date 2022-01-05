package com.zmh.fastlog.worker;

import java.io.Closeable;

/**
 * @author zmh
 *
 * 消息发送接口, 无阻塞
 */
public interface Worker<T> extends Closeable {

    boolean sendMessage(T message);

    void close();
}

