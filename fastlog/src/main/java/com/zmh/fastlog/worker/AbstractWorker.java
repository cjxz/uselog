package com.zmh.fastlog.worker;

import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;

public abstract class AbstractWorker<MESSAGE, EVENT> implements SequenceReportingEventHandler<EVENT>, Worker<MESSAGE> {

    @Override
    public void onEvent(EVENT event, long sequence, boolean endOfBatch) {
        dequeue(event, sequence, endOfBatch);
    }

    @Override
    public boolean sendMessage(MESSAGE message) {
        return enqueue(message);
    }

    protected abstract boolean enqueue(MESSAGE message);

    protected abstract void dequeue(EVENT event, long sequence, boolean endOfBatch);

    protected Sequence sequenceCallback;

    @Override
    public void setSequenceCallback(Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }
}
