package com.zmh.fastlog.config;

import lombok.Data;

@Data
public class FastLogConfig {
    private String url;
    private String topic;
    private boolean enable;
    private int logBatchHandlerSize; //一批处理多少条日志
    private String type; //kafka pulsar
    private int kafkaPartition;
    private String fileCacheFolder;
    private int fileMemoryCacheSize; //bytes 单位：字节
    private int logMaxSize; //一条最多能占用多少字节，多于这个数量 截取或者丢弃
    private String largeLogHandlerType; // cut截取 discard丢弃

    public int getBatchSize() {
        if ("kafka".equals(type)) {
            return logBatchHandlerSize * kafkaPartition;
        } else {
            return logBatchHandlerSize;
        }
    }
}
