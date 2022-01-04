package com.zmh.fastlog.worker;

import com.zmh.fastlog.producer.ByteEvent;
import lombok.Data;

import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.lang.System.arraycopy;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Data
public class DataByteMessage extends ByteMessage {
    private byte[] data;
    private int dataLength;

    public DataByteMessage(long id, byte[] data) {
        this.setId(id);
        this.data = data;
        this.dataLength = nonNull(data) ? data.length : 0;
    }

    public DataByteMessage(long id, byte[] data, int len) {
        this.setId(id);
        this.data = data;
        this.dataLength = len;
    }

    public void apply(ByteEvent event) {
        event.clear();
        if (isNull(data) || 0 == dataLength) {
            return;
        }
        event.setId(this.getId());
        ByteBuffer buffer = event.getBuffer();
        if (isNull(buffer) || buffer.capacity() < dataLength) {
            buffer = ByteBuffer.allocate(marginToBuffer(dataLength));
            event.setBuffer(buffer);
        }
        arraycopy(data, 0, buffer.array(), 0, dataLength);
        event.setBufferLen(dataLength);

        buffer.limit(dataLength);
    }

}
