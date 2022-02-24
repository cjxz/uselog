package com.zmh.fastlog.config;

import lombok.Data;

/**
 * fastlog配置信息，FastLogConfig的赋值由logbook实现，配置文件地址在 resources/fastlog-base.xml
 */
@Data
public class FastLogConfig {
    private String url;
    private String topic;

    /**
     * 是否使用fastlog收集日志
     * 默认：false
     */
    private boolean enable;

    /**
     * mq一批发送多少条日志
     * 默认：1024
     */
    private int batchMessageSize;

    /**
     * 使用什么类型的mq框架收集日志 支持：kafka 和 pulsar
     * 默认：kafka
     */
    private String mqType;

    /**
     * kafka的分片数量
     * 默认：4
     */
    private int kafkaPartition;

    /**
     * 日志临时存入磁盘文件的目录地址
     * 默认：logs/cache
     */
    private String fileCacheFolder;

    /**
     * 存入磁盘文件的内存缓存区大小，实际开的内存缓存区大小是这个数值的3倍
     * 单位：字节
     * 默认：64 * 1024 * 1024 即 64MB
     */
    private int fileMemoryCacheSize;

    /**
     * 一个日志磁盘文件中最多能存放多少个缓存块，必须为2的幂数
     * 默认：16
     */
    private int fileMaxCacheCount;

    /**
     * 最多能有多少个日志文件，每个日志文件的大小是fileMemoryCacheSize * fileMaxCacheCount的大小，也就是磁盘最多能占用maxFileCount * fileMemoryCacheSize * fileMaxCacheCount的大小，
     * 超过这个数量，则删除最老的日志文件
     * <p>
     * 默认：20 （也就是默认磁盘最多占用 20 * 64 * 16MB = 20GB）
     * <p>
     * 假设mq挂了，日志的平均收集速度是1w/QPS，每条日志的大小是256字节，那么磁盘占满需要 20GB / (256 * 1w) ≈ 8192s = 2.28h
     */
    private int maxFileCount;

    /**
     * 日志的message最多能占用多少字符，多于这个数量截取
     * 单位：字符
     * 默认：10240
     */
    private int maxMsgSize;

    /**
     * Kafka的batchsize单位是字节
     * 根据默认的batchMessageSize来计算，128 * 512 = 64kB，64kB是一个Kafka分片对应的缓存区大小，
     * 所以Kafka做异步发送用于缓存消息的内存大小是 64kB * kafkaPartition。
     * 由于fastlog中kafka总体内存使用大小使用kafka的默认配置是32MB，其中要留存部分内存用于kafka消息压缩等，可供缓存消息的内存设置最大为10MB，
     * 所以不管batchMessageSize 和 kafkaPartition 如何配置，相乘后的数值不能大于10MB
     * <p>
     * pulsar的batchsize单位是日志条数
     */
    public int getBatchSize() {
        if ("kafka".equals(mqType)) {
            int messageBufferSize = 10 * 1024 * 1024;
            int partitionSize = batchMessageSize * Math.min(maxMsgSize, 512); //512是根据经验预估的一条日志的平均大小

            if (partitionSize * kafkaPartition < messageBufferSize) {
                return Math.min(partitionSize, 100_0000);
            }
            return Math.min(messageBufferSize / kafkaPartition, 100_0000);
        } else {
            return batchMessageSize;
        }
    }
}
