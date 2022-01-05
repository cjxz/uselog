package com.zmh.fastlog.model.event;

import lombok.Data;

import java.nio.ByteBuffer;

import static java.util.Objects.nonNull;

@Data
public class ByteEvent {
    private long id;
    private ByteBuffer buffer;
    private int bufferLen;

    public void clear() {
        if (nonNull(buffer)) {
            buffer.clear();
        }
        bufferLen = 0;
        id = 0;
    }
}
