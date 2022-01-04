package com.zmh.fastlog.worker;

import com.zmh.fastlog.producer.ByteEvent;
import lombok.Data;
import lombok.Value;

import java.io.Closeable;
import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.lang.System.arraycopy;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * @author zmh
 */
public interface Worker<T> extends Closeable {
    /**
     * 消息发送接口, 无阻塞
     *
     * @param message
     * @return
     */
    boolean sendMessage(T message);

    void close();

}

@Data
abstract class ByteMessage {
    private long id;

    public abstract void apply(ByteEvent event);

}

@Value
class LastSeq {
    private long seq;
}
