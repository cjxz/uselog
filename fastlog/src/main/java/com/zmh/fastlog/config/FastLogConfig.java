package com.zmh.fastlog.config;

import lombok.Data;

import java.util.Map;

@Data
public class FastLogConfig {
    private String url;
    private String topic;
    private boolean enable;
    private int batchMessageSize;
    private String type;//kafka pulsar
    private Map<String, String> additionalFields;
    private int partition;

    public int getBatchSize() {
        if ("kafka".equals(type)) {
            return batchMessageSize * partition;
        } else {
            return batchMessageSize;
        }
    }
}
