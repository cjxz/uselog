package com.zmh.fastlog.model.event;

import com.zmh.fastlog.utils.JsonByteBuilder;
import com.zmh.fastlog.model.message.AbstractMqMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class LogDisruptorEvent extends AbstractMqMessage {
    private final static int INIT_SIZE = 2048;

    private JsonByteBuilder byteBuilder = JsonByteBuilder.create(INIT_SIZE);
    private WeakReference<JsonByteBuilder> cache = new WeakReference<>(byteBuilder);

    public void clear() {
        if (byteBuilder != null) {
            if (byteBuilder.capacity() > INIT_SIZE) {
                cache = new WeakReference<>(byteBuilder);
                byteBuilder = null;
            } else {
                byteBuilder.clear();
            }
        }
    }

    public JsonByteBuilder getByteBuilder() {
        if (null == byteBuilder) {
            byteBuilder = cache.get();
            if (null == byteBuilder) {
                byteBuilder = JsonByteBuilder.create(INIT_SIZE);
            }
        }
        return byteBuilder;
    }

    @Override
    public void apply(ByteEvent event) {
        event.clear();

        ByteBuffer buff = byteBuilder.toByteBuffer(event.getBuffer());
        event.setId(this.getId());
        event.setBuffer(buff);
        event.setBufferLen(buff.position());
    }
}
