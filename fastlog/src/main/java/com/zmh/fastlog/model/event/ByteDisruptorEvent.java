package com.zmh.fastlog.model.event;


import java.lang.ref.SoftReference;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class ByteDisruptorEvent {

    private SoftReference<ByteEvent> byteEventRef = new SoftReference<>(new ByteEvent());

    private ByteEvent refKeep;

    public ByteEvent getByteEvent() {
        if (nonNull(refKeep)) {
            return refKeep;
        }
        ByteEvent event = this.byteEventRef.get();
        if (isNull(event)) {
            event = new ByteEvent();
            byteEventRef = new SoftReference<>(event);
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
}
