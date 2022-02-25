package com.zmh.fastlog;

import io.appulse.utils.Bytes;
import org.apache.pulsar.shade.io.airlift.compress.lz4.Lz4Compressor;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Calendar;

import static com.zmh.fastlog.utils.Utils.debugLogCondition;
import static com.zmh.fastlog.worker.file.fifo.ReadWriteFileFactory.createReadFile;

public class TestDate {

    @Test
    public void test() {
        Calendar calendar = new Calendar.Builder().build();
        calendar.setTimeInMillis(1643828583000L);

        int year = calendar.get(Calendar.YEAR);

        int month = calendar.get(Calendar.MONTH) + 1;

        int day = calendar.get(Calendar.DAY_OF_MONTH);

        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        int minute = calendar.get(Calendar.MINUTE);

        int seconds = calendar.get(Calendar.SECOND);

        System.out.println(String.format("%d,%d,%d,%d,%d,%d", year, month, day, hour, minute, seconds));
    }

    /*@Test
    public void test1() {
        Bytes bytes = Bytes.allocate(2000);

        Lz4Compressor compressor = new Lz4Compressor();

        int compress = compressor.compress(bytes.array(), 0, bytes.readableBytes(), compressorBuffer, 0, compressorBuffer.length);
        debugLogCondition("compress, before" + bytes.readableBytes() + ",after" + compress);

        ByteBuffer buffer = ByteBuffer.wrap(compressorBuffer);
        buffer.position(0);
        buffer.limit(compress);

        if (writeFile.write(buffer)) {
            return;
        }
        if (filesManager.getFileNum() == 1) {
            readFile = createReadFile(writeFile.getPath(), indexFile, writeFile.getReadIndex(), writeFile.getWriteIndex(), capacity);
        }
    }*/
}

