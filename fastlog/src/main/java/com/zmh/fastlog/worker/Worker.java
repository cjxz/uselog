package com.zmh.fastlog.worker;

/**
 * @author zmh
 *
 * 消息发送接口, 无阻塞
 */
public interface Worker<MESSAGE> extends AutoCloseable {

    boolean sendMessage(MESSAGE message);

    void close();
}

