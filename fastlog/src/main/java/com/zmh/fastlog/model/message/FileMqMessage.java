package com.zmh.fastlog.model.message;

import com.zmh.fastlog.model.event.ByteEvent;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.ByteBuffer;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static java.util.Objects.isNull;

@EqualsAndHashCode(callSuper = true)
@Data
public class FileMqMessage extends AbstractMqMessage {
    private byte[] data;
    private int dataLength;

    public FileMqMessage(long id, byte[] data, int len) {
        this.setId(id);
        this.data = data;
        this.dataLength = len;
    }

    public void apply(ByteEvent event) {
        event.clear();
        if (isNull(data) || 0 == dataLength) {
            return;
        }
        long id = this.getId();
        event.setId(id);
        if (id == 0L) {
            System.out.println("file set id " + id);
        }

        ByteBuffer buffer = event.getBuffer();
        if (isNull(buffer) || buffer.capacity() < dataLength) {
            buffer = ByteBuffer.allocate(marginToBuffer(dataLength));
            event.setBuffer(buffer);
        }
        buffer.put(data, 0, dataLength);
    }

}
