package com.zmh.fastlog.model.event;


import com.zmh.fastlog.model.message.ByteData;

import java.lang.ref.SoftReference;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class EventSlot {
    private SoftReference<ByteData> softRef = new SoftReference<>(new ByteData());

    private ByteData refKeep;

    public ByteData getByteData() {
        if (nonNull(refKeep)) {
            return refKeep;
        }
        ByteData event = this.softRef.get();
        if (isNull(event)) {
            event = new ByteData();
            softRef = new SoftReference<>(event);
        }
        refKeep = event;
        return event;
    }

    public void clear() {
        if (nonNull(refKeep)) {
            if (refKeep.capacity() > 2048) {
                softRef = new SoftReference<>(new ByteData());
                refKeep = null;
            }
        }
    }
}
