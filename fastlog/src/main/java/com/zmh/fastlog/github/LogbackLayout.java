package com.zmh.fastlog.github;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import com.alibaba.fastjson.JSONObject;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LogbackLayout extends LayoutBase<ILoggingEvent> {

    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");


    public String doLayout(ILoggingEvent event) {
        JSONObject root = new JSONObject();
        root.put("seq", 12345);
        root.put("message", event.getFormattedMessage());
        root.put("logger", event.getLoggerName());
        root.put("thread", event.getThreadName());
        root.put("level", event.getLevel().levelStr);

        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneId.systemDefault());
        root.put("@timestamp", dateTimeFormatter.format(localDateTime));
        root.put("ts", event.getTimeStamp());

        return root.toString();
    }

}
