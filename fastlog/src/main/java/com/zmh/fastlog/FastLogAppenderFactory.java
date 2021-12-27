package com.zmh.fastlog;

import com.zmh.fastlog.config.FastLogConfig;
import com.zmh.fastlog.producer.KafkaEventProducer;
import com.zmh.fastlog.producer.MqEventProducer;
import com.zmh.fastlog.producer.PulsarEventProducer;
import com.zmh.fastlog.worker.FileQueueWorker;
import com.zmh.fastlog.worker.LogWorker;
import com.zmh.fastlog.worker.MqEventWorker;
import com.zmh.fastlog.worker.Worker;
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
    private WorkerRef<Object> pulsarWorker = new WorkerRef<>();
    private WorkerRef<Object> fileQueueWorker = new WorkerRef<>();

    public AppenderFacade(FastLogConfig config) {
        try {
            fileQueueWorker.setDelegate(new FileQueueWorker(pulsarWorker));

            logWorker.setDelegate(new LogWorker(pulsarWorker, fileQueueWorker, config.getBatchSize(), config.getAdditionalFields()));

            MqEventProducer producer = null;
            if ("kafka".equals(config.getType())) {
                producer = new KafkaEventProducer(config.getUrl(), config.getTopic(), config.getBatchMessageSize());
            } else if ("pulsar".equals(config.getType())) {
                producer = new PulsarEventProducer(config.getUrl(), config.getTopic(), config.getBatchMessageSize());
            }
            pulsarWorker.setDelegate(new MqEventWorker(logWorker, producer, config.getBatchSize()));
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
        pulsarWorker.close();
        logWorker.close();
        fileQueueWorker.close();
    }
}
