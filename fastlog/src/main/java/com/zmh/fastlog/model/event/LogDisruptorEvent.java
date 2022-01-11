package com.zmh.fastlog.model.event;

import com.zmh.fastlog.model.message.AbstractMqMessage;
import com.zmh.fastlog.utils.JsonByteBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.Utils.marginToBuffer;

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

        ByteBuffer buff = event.getBuffer();
        if (buff == null || buff.capacity() < byteBuilder.pos()) {
            int len = marginToBuffer(byteBuilder.pos());
            buff = ByteBuffer.wrap(new byte[len]);
        }
        buff.put(byteBuilder.array(), 0, byteBuilder.pos());

        long id = this.getId();
        event.setId(id);
        if (id == 0L) {
            System.out.println("log set id " + id);
        }
        event.setBuffer(buff);
    }
}
