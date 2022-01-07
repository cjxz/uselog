package com.zmh.fastlog.config;

import lombok.Data;

@Data
public class FastLogConfig {
    private String url;
    private String topic;
    private boolean enable;
    private int batchMessageSize; //一批处理多少条日志
    private String type; //kafka pulsar
    private int kafkaPartition;
    private String fileCacheFolder;
    private int fileMemoryCacheSize; //bytes 单位：字节
    private int maxMsgSize; //日志的message最多能占用多少字符，多于这个数量截取 单位：字符

    public int getBatchSize() {
        if ("kafka".equals(type)) {
            int size = batchMessageSize * Math.min(maxMsgSize, 512); //512是根据经验预估的一条日志的平均大小

            if (size * kafkaPartition < 10 * 1024 * 1024) {
                return size;
            }
            return 10 * 1024 * 1024 / kafkaPartition;
        } else {
            return batchMessageSize;
        }
    }
}
