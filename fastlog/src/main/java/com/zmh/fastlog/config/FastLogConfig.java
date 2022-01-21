package com.zmh.fastlog.config;

import ch.qos.logback.core.util.FileSize;
import lombok.Data;

import static com.zmh.fastlog.utils.Utils.debugLog;

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
     * 默认：128
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

    private long totalSizeCap;
    /**
     * 最多能有多少个日志文件，每个日志文件的大小就是fileMemoryCacheSize的大小，也就是磁盘最多能占用maxFileCount * fileMemoryCacheSize的大小，
     * 超过这个数量，则删除最老的日志文件
     * <p>
     * 默认：100 （也就是默认磁盘最多占用 100 * 64MB = 6.4GB）
     * <p>
     * 假设mq挂了，日志的收集速度是40w/QPS，每条日志的大小是512字节，那么磁盘占满需要 6.4GB / (512 * 40w) ≈ 33秒 // todo zmh 33秒太短了？？
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
                return partitionSize;
            }
            return messageBufferSize / kafkaPartition;
        } else {
            return batchMessageSize;
        }
    }

    public void setTotalSizeCap(FileSize fileSize) {
        debugLog("setting totalSizeCap to " + fileSize.toString());
        this.totalSizeCap = fileSize.getSize();

        if (Long.bitCount(totalSizeCap) != 1) {
            throw new IllegalArgumentException("totalSizeCap must be a power of 2");
        }
    }
}
