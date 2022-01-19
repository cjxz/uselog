package com.zmh.fastlog;

import com.zmh.fastlog.config.FastLogConfig;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.worker.Worker;
import com.zmh.fastlog.worker.file.FileWorker;
import com.zmh.fastlog.worker.log.LogWorker;
import com.zmh.fastlog.worker.mq.MqWorker;
import com.zmh.fastlog.worker.mq.producer.KafkaProducer;
import com.zmh.fastlog.worker.mq.producer.MqProducer;
import com.zmh.fastlog.worker.mq.producer.PulsarProducer;
import lombok.Setter;

import static java.util.Objects.nonNull;

/**
 * @author zmh
 */
public class FastLogAppenderFactory {
    public static Worker<Object> create(FastLogConfig config) {
        return new AppenderFacade(config);
    }
}

class WorkerRef<T> implements Worker<T> {

    @Setter
    private Worker<T> delegate;

    @Override
    public boolean sendMessage(T message) {
        if (nonNull(delegate)) {
            return delegate.sendMessage(message);
        }
        return false;
    }

    @Override
    public void close() {
        if (nonNull(delegate)) {
            delegate.close();
            delegate = null;
        }
    }

}

class AppenderFacade implements Worker<Object> {
    private WorkerRef<Object> logWorker = new WorkerRef<>();
    private WorkerRef<ByteData> mqWorker = new WorkerRef<>();
    private WorkerRef<ByteData> fileWorker = new WorkerRef<>();

    public AppenderFacade(FastLogConfig config) {
        try {
            fileWorker.setDelegate(new FileWorker(mqWorker, config.getFileMemoryCacheSize(), config.getMaxFileCount(), config.getFileCacheFolder()));

            logWorker.setDelegate(new LogWorker(mqWorker, fileWorker, config.getBatchMessageSize(), config.getMaxMsgSize()));

            MqProducer producer;
            if ("pulsar".equals(config.getMqType())) {
                producer = new PulsarProducer(config.getUrl(), config.getTopic(), config.getBatchSize());
            } else {
                producer = new KafkaProducer(config.getUrl(), config.getTopic(), config.getBatchSize());
            }
            mqWorker.setDelegate(new MqWorker(logWorker, producer, config.getBatchMessageSize()));
        } catch (Exception ex) {
            this.close();
            throw ex;
        }

    }

    @Override
    public boolean sendMessage(Object message) {
        return logWorker.sendMessage(message);
    }

    @Override
    public void close() {
        mqWorker.close();
        logWorker.close();
        fileWorker.close();
    }
}
