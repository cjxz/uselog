package com.zmh.fastlog.producer;

import java.lang.ref.SoftReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class MqEvent {

    private SoftReference<ByteEvent> byteEvent = new SoftReference<>(new ByteEvent());

    private ByteEvent refKeep;

    public ByteEvent getByteEvent() {
        if (nonNull(refKeep)) {
            return refKeep;
        }
        ByteEvent event = this.byteEvent.get();
        if (isNull(event)) {
            event = new ByteEvent();
            byteEvent = new SoftReference<>(event);
        }
        refKeep = event;
        return event;
    }

    public void clear() {
        if (nonNull(refKeep)) {
            refKeep.clear();
            refKeep = null;
        }
    }

    public String getEventString() {
        if (isNull(refKeep)) {
            return "";
        }
        return new String(refKeep.getBuffer().array(), 0, refKeep.getBufferLen(), UTF_8);
    }
}
