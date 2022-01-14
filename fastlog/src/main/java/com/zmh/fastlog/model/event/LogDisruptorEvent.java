package com.zmh.fastlog.model.event;

import com.zmh.fastlog.model.message.AbstractMqMessage;
import com.zmh.fastlog.utils.JsonByteBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static java.util.Objects.nonNull;

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

    /**
     * event 中的 buffer 中的byte[] 跟 byteBuilder 中的byte[] 互换引用
     */
    @Override
    public void apply(ByteEvent event) {
        event.clear();

        if (byteBuilder.pos() <= 0) {
            debugLog("LogDisruptorEvent apply error pos" + byteBuilder.pos());
        }

        byte[] temp = byteBuilder.array();
        ByteBuffer buff = event.getBuffer();
        if (nonNull(buff)) {
            byteBuilder.array(buff.array());
        } else {
            byteBuilder.array(new byte[INIT_SIZE]);
        }
        buff = ByteBuffer.wrap(temp, 0, byteBuilder.pos());

        event.setId(this.getId());
        event.setBuffer(buff);
    }
}
