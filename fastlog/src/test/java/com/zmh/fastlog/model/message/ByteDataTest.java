package com.zmh.fastlog.model.message;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.zmh.fastlog.utils.Utils.debugLog;
import static com.zmh.fastlog.utils.Utils.getText;

public class ByteDataTest {

    @Test
    public void compare() {
        List<Integer> integers = Arrays.asList(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000);

        for (Integer size : integers) {
            byte[] bytes = getText(size).getBytes();

            testApply(bytes);
            testSwitch(bytes);
        }
    }

    //@Test
    public void testSwitch(byte[] bytes) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ByteData byteData = new ByteData();
        byteData.setData(bytes);
        byteData.setDataLength(bytes.length);

        for (long i = 0; i < 1000_0000; i++) {
            byteData.setId(i);

            byteData.switchData(new ByteData());
        }

        stopWatch.stop();
        debugLog("switch " + bytes.length + " " + stopWatch.formatTime());
    }

    //@Test
    public void testApply(byte[] bytes) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ByteData byteData = new ByteData();
        byteData.setData(bytes);
        byteData.setDataLength(bytes.length);

        for (long i = 0; i < 1000_0000; i++) {
            byteData.setId(i);

            byteData.apply(new ByteData());
        }

        stopWatch.stop();

        debugLog("apply " + bytes.length + " " + stopWatch.formatTime());
    }
}
