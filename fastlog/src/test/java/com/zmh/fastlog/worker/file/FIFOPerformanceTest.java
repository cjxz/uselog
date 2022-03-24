package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.Utils;
import com.zmh.fastlog.worker.BeforeDeleteFile;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class FIFOPerformanceTest extends BeforeDeleteFile {

    @Test
    public void testFIFOQueue() {
        //先写内存，再批量写磁盘，每个文件64MB
        try (FIFOQueue fifo = new FIFOQueue("logs/cache", 64, 1024, 100, "lz4")) {
            execute(fifo);
        }
    }

    private void execute(FIFOQueue fifo) {
        StopWatch watch = new StopWatch();
        long seq = 1L;

        byte[] bytes = Utils.getText(200).getBytes();
        ByteData byteEvent = new ByteData(0, bytes, bytes.length);

        watch.start();
        int total = 1000;
        for (int i = 0; i < total; i++) {
            for (int j = 0; j < 10000; j++) {
                byteEvent.setId(seq++);
                fifo.put(byteEvent);
            }

            for (int j = 0; j < 1000; j++) {
                fifo.get();
                fifo.next();
            }
        }
        watch.stop();
        System.out.println("一共创建了" + fifo.getTotalFile() + "个文件，还剩余" + fifo.getFileNum() + "个文件");
        System.out.println(watch.formatTime());
        System.out.println(total / watch.getTime(TimeUnit.SECONDS) + "w/QPS");
    }

}
