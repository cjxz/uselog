package com.zmh.fastlog.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class DateSequenceTest {
    private final int EXPECT_BATCH_COUNT = 1000;
    private final int EXPECT_BATCH_SIZE = 10000;
    private final int EXPECT_COUNT = EXPECT_BATCH_COUNT * EXPECT_BATCH_SIZE;
    private final long currentTime = 1641744000000L;


    @Before
    public void before() {
        ImpureUtils.setCurrentTimeMillis(currentTime); // 2022-01-10 00:00:00
    }

    @Test
    public void getNextOneTimeTest() {
        DateSequence sequence = new DateSequence();

        long next = sequence.next();

        assertEquals("(1641744000000, 0, 1)", DateSequence.toString(next));
        assertEquals(currentTime, DateSequence.getMillSec(next));
        assertEquals(0, DateSequence.getBizId(next));
    }

    @Test
    public void getNextTwoTimesTest() {
        DateSequence sequence = new DateSequence();

        long next1 = sequence.next();
        long next2 = sequence.next();

        assertEquals(DateSequence.getMillSec(next1), DateSequence.getMillSec(next2));
        assertEquals(DateSequence.getBizId(next1), DateSequence.getBizId(next2));
        assertEquals(1, DateSequence.getSeq(next2) - DateSequence.getSeq(next1));
    }

    @Test
    public void getBatchNextTwoTimesTest() {
        DateSequence sequence = new DateSequence();

        long next1 = sequence.next(4000);
        long next2 = sequence.next(4000);

        assertEquals(1, DateSequence.getMillSec(next2) - DateSequence.getMillSec(next1));
        assertEquals(4000, DateSequence.getSeq(next1));
        assertEquals(4000 - (4096 - 4000), DateSequence.getSeq(next2));
    }

    @Test
    public void greaterThan4096QPMSTest() { // 测试每毫秒超过4096的情况
        DateSequence sequence = new DateSequence();

        long[] array = new long[EXPECT_COUNT];
        IntStream.range(0, EXPECT_COUNT)
            .parallel()
            .forEach(i -> array[i] = sequence.next());

        Arrays.parallelSort(array);

        long first = DateSequence.getMillSec(array[0]);
        long end = DateSequence.getMillSec(array[array.length - 1]);
        assertEquals(EXPECT_COUNT / 4096, end - first);

        for (int i = 1; i < array.length; i++) {
            if (array[i - 1] == array[i]) {
                Assert.fail();
            }
        }
    }

    @Test
    public void parallelNextTest() {
        ImpureUtils.reset();

        long[] array = new long[EXPECT_COUNT];
        DateSequence sequence = new DateSequence(123);
        IntStream.range(0, EXPECT_BATCH_COUNT)
            .parallel()
            .forEach(i -> {
                    int index = i * EXPECT_BATCH_SIZE;
                    for (int j = 0; j < EXPECT_BATCH_SIZE; j++) {
                        long v = sequence.next();
                        array[index++] = v;
                    }
                }
            );

        Arrays.parallelSort(array);
        assertTrue(array[array.length - 1] > 0);

        for (int i = 1; i < array.length; i++) {
            if (array[i - 1] == array[i]) {
                Assert.fail();
            }
        }
    }

    @Test
    public void parallelBatchNextTest() {
        DateSequence sequence = new DateSequence();
        long[] array = IntStream.range(0, EXPECT_COUNT)
            .parallel()
            .mapToLong(i -> sequence.next(5))
            .toArray();
        assertThat(array.length, equalTo(EXPECT_COUNT));

        Arrays.parallelSort(array);

        long first = DateSequence.getMillSec(array[0]);
        long end = DateSequence.getMillSec(array[array.length - 1]);
        assertEquals(EXPECT_COUNT * 5 / 4096, end - first);

        for (int i = 1; i < array.length; i++) {
            if (array[i - 1] == array[i]) {
                Assert.fail();
            }
        }
    }
}