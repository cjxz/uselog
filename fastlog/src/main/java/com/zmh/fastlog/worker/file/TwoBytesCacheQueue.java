package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.worker.DataByteMessage;
import lombok.Getter;

public class TwoBytesCacheQueue {
    @Getter
    private BytesCacheQueue used;
    private BytesCacheQueue other;

    public TwoBytesCacheQueue(int size) {
        used = new BytesCacheQueue(size);
        other = new BytesCacheQueue(size);
    }

    public void switchHead() {
        other.checkPut();
        other.reset();
        BytesCacheQueue temp = used;
        used = other;
        other = temp;
    }

    public boolean put(ByteEvent event) {
        return used.put(event);
    }

    public DataByteMessage get() {
        return used.get();
    }

    public void reset() {
        this.used.reset();
    }

    public boolean isEmpty() {
        return this.used.isEmpty();
    }

    public void copyTo(BytesCacheQueue queue) {
        this.used.copyTo(queue);
    }
}
