package com.zmh.ringbuffer.demo.ring;


import com.google.common.util.concurrent.RateLimiter;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import lombok.NonNull;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static com.zmh.ringbuffer.demo.ring.ThreadUtils.namedDaemonThreadFactory;
import static com.zmh.ringbuffer.demo.ring.ThreadUtils.scheduleAtFixedRate;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Ring1 implements EventHandler<Event> {

    private final RingBuffer<Event> ringBuffer;
    private final Disruptor<Event> queue;
    private final Ring2 ring2;

    private final LongAdder missingCount = new LongAdder();
    private final ScheduledFuture<?> missingSchedule = scheduleAtFixedRate(() -> {
        long sum = missingCount.sumThenReset();
        if (sum > 0) {
            System.out.println("Ring1 mission count:" + sum);
        }
    }, 5, 5, SECONDS);

    public Ring1() {
        queue = new Disruptor<>(
            Event::new,
            2048,
            namedDaemonThreadFactory("Ring1"),
            ProducerType.SINGLE,
            new LiteBlockingWaitStrategy()
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();

        ring2 = new Ring2();
    }

    private RateLimiter limiter = RateLimiter.create(10_0000);

    public void sendMsg() {
        boolean b = ringBuffer.tryPublishEvent((e, s) -> {
            limiter.acquire();
        });
        if (!b) {
            missingCount.increment();
        }
    }

    @Override
    public void onEvent(Event event, long sequence, boolean endOfBatch) {
        ring2.sendMsg();
    }
}

class Ring2 implements EventHandler<Event> {

    private final RingBuffer<Event> ringBuffer;
    private final Disruptor<Event> queue;

    private final LongAdder missingCount = new LongAdder();
    private final ScheduledFuture<?> missingSchedule = scheduleAtFixedRate(() -> {
        long sum = missingCount.sumThenReset();
        if (sum > 0) {
            System.out.println("Ring2 mission count:" + sum);
        }
    }, 5, 5, SECONDS);

    public Ring2() {
        queue = new Disruptor<>(
            Event::new,
            2048,
            namedDaemonThreadFactory("Ring2"),
            ProducerType.SINGLE,
            new LiteBlockingWaitStrategy()
        );
        queue.handleEventsWith(this);
        ringBuffer = queue.getRingBuffer();
        queue.start();
    }

    private RateLimiter limiter5 = RateLimiter.create(9_0000);

    public void sendMsg() {
        boolean b = ringBuffer.tryPublishEvent((e, s) -> {
            limiter5.acquire();
        });
        if (!b) {
            missingCount.increment();
        }
    }

    private RateLimiter limiter3 = RateLimiter.create(1_0000);

    @Override
    public void onEvent(Event event, long sequence, boolean endOfBatch) {
        limiter3.acquire();

        /*if (ringBuffer.getCursor() - sequence == 0) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

            System.out.println("ring2 end" + sdf.format(new Date()));
        }*/
    }
}


@Data
class Event {
    private String msg;
}


class ThreadUtils {

    private final static ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(5);

    public static ScheduledFuture<?> scheduleAtFixedRate(Runnable run, long initialDelay, long period, TimeUnit unit) {
        return scheduledPool.scheduleAtFixedRate(run, initialDelay, period, unit);
    }

    public static ThreadFactory namedDaemonThreadFactory(String name) {
        return new ThreadFactory() {
            private volatile int index = 0;

            @Override
            public Thread newThread(@NonNull Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName(name + '-' + index++);
                return t;
            }
        };
    }
}