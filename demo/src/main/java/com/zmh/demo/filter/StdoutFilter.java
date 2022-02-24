package com.zmh.demo.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.springframework.stereotype.Component;

@Component
public class StdoutFilter extends Filter<ILoggingEvent> {

    @Override
    public FilterReply decide(ILoggingEvent event) {
        String loggerName = event.getLoggerName();
        return loggerName.startsWith("com.zmh.demo.controller") ?  FilterReply.DENY : FilterReply.NEUTRAL;
    }
}
