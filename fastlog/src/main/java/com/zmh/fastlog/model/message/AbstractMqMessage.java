package com.zmh.fastlog.model.message;

import com.zmh.fastlog.model.event.ByteEvent;
import lombok.Data;

@Data
public abstract class AbstractMqMessage {
    private long id;

    public abstract void apply(ByteEvent event);
}
