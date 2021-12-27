package com.zmh.fastlog.worker;

import com.google.common.collect.ImmutableList;
import com.zmh.fastlog.producer.ByteEvent;
import com.zmh.fastlog.worker.DbIndex.Range;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;

import static com.zmh.fastlog.utils.ThreadUtils.sleep;
import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static org.apache.pulsar.shade.org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.pulsar.shade.org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author zmh
 */
public class FileQueueWorkerTest {

    @Before
    public void cleanUp() {
        FileQueueWorker.cleanUp();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWhenPulsarNormal() {
        Worker<Object> pulsarWorker = mock(Worker.class);
        when(pulsarWorker.sendMessage(any())).thenReturn(true);
        try (FileQueueWorker queue = new FileQueueWorker(pulsarWorker)) {
            sleep(500);
            // timeout never sendMessage when buffer is empty
            verify(pulsarWorker, never()).sendMessage(any());

            queue.sendMessage(new DataByteMessage(10, new byte[13]));
            verify(pulsarWorker, timeout(500).only()).sendMessage(any());

            reset(pulsarWorker);
            when(pulsarWorker.sendMessage(any())).thenReturn(true);
            sleep(500);
            verify(pulsarWorker, never()).sendMessage(any());

            InOrder inOrder = inOrder(pulsarWorker);
            queue.sendMessage(new DataByteMessage(11, new byte[13]));
            queue.sendMessage(new DataByteMessage(12, new byte[13]));

            verify(pulsarWorker, timeout(500).times(1)).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 11));
            inOrder.verify(pulsarWorker).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 11));
            inOrder.verify(pulsarWorker).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 12));

            reset(pulsarWorker);
            when(pulsarWorker.sendMessage(any())).thenReturn(true);
            sleep(500);
            verify(pulsarWorker, never()).sendMessage(any());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWhenPulsarBlocked() {
        Worker<Object> pulsarWorker = mock(Worker.class);
        when(pulsarWorker.sendMessage(any())).thenReturn(false);
        try (FileQueueWorker queue = new FileQueueWorker(pulsarWorker)) {
            queue.sendMessage(new DataByteMessage(10, new byte[13]));
            queue.sendMessage(new DataByteMessage(11, new byte[13]));

            verify(pulsarWorker, timeout(99)).sendMessage(any());
            reset(pulsarWorker);
            sleep(500);
            verify(pulsarWorker, atLeast(4)).sendMessage(any());

            reset(pulsarWorker);
            when(pulsarWorker.sendMessage(any())).thenReturn(true);
            verify(pulsarWorker, timeout(500).times(1)).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 10));
            InOrder inOrder = inOrder(pulsarWorker);
            inOrder.verify(pulsarWorker).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 10));
            inOrder.verify(pulsarWorker).sendMessage(argThat(msg -> ((ByteMessage) msg).getId() == 11));

        }
    }

    public static class FIFOFileTest {

        @Before
        public void cleanUp() {
            FIFOFile.cleanUp();
        }

        @Test
        public void testEmptyFile() {
            try (FIFOFile fifo = new FIFOFile()) {
                assertNull(fifo.get());
                fifo.next();
                assertNull(fifo.get());
            }
        }

        private void verifyByteMessage(ByteMessage message, long id, int len, byte[] arr) {
            ByteEvent e = new ByteEvent();
            assertNotNull(message);
            message.apply(e);
            assertEquals(id, e.getId());
            assertNotNull(e.getBuffer());
            assertEquals(len, e.getBufferLen());
            if (nonNull(arr)) {
                byte[] actual = e.getBuffer().array();
                assertNotNull(actual);
                for (int i = 0; i < len; i++) {
                    assertEquals(arr[i], actual[i]);
                }
            }
        }

        private byte[] array(int len, int... eles) {
            byte[] bytes = new byte[len];
            for (int i = 0; i < eles.length; i++) {
                bytes[i] = (byte) eles[i];
            }
            return bytes;
        }

        @Test
        public void testMessagePersistence() {
            try (FIFOFile fifo = new FIFOFile()) {
                fifo.addItem(1, array(17, 3, 2, 4, 5), 17);
                fifo.next();
                ByteMessage message = fifo.get();
                verifyByteMessage(message, 1, 17, array(17, 3, 2, 4, 5));

                fifo.next();
                message = fifo.get();
                assertNull(message);

                fifo.addItem(2, array(19, 9, 8, 7, 6, 5, 1, 2, 3), 19);
                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 2, 19, array(19, 9, 8, 7, 6, 5, 1, 2, 3));
            }
            try (FIFOFile fifo = new FIFOFile()) {
                fifo.next();
                ByteMessage message = fifo.get();
                verifyByteMessage(message, 1, 17, array(17, 3, 2, 4, 5));

                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 2, 19, array(19, 9, 8, 7, 6, 5, 1, 2, 3));
            }
        }

        @Test
        public void deleteTest() {
            try (FIFOFile fifo = new FIFOFile()) {
                fifo.addItem(1, new byte[17], 17);
                fifo.addItem(2, new byte[19], 19);
                fifo.addItem(3, new byte[23], 23);
                fifo.addItem(4, new byte[29], 29);
                fifo.addItem(5, new byte[29], 29);
                fifo.addItem(6, new byte[29], 29);
                fifo.addItem(7, new byte[29], 29);
                fifo.addItem(8, new byte[29], 29);

                // current 0
                fifo.deleteBeforeId(1);
                fifo.next();
                ByteMessage message = fifo.get();
                verifyByteMessage(message, 2, 19, null);

                // current 2
                fifo.deleteBeforeId(2);
                message = fifo.get();
                assertNull(message);

                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 3, 23, null);

                // current 3
                fifo.deleteBeforeId(5);
                message = fifo.get();
                assertNull(message);

                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 6, 29, null);

                // current 6
                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 7, 29, null);

                // current 7
                fifo.deleteBeforeId(6);
                message = fifo.get();
                verifyByteMessage(message, 7, 29, null);

                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 8, 29, null);
            }
            try (FIFOFile fifo = new FIFOFile()) {
                fifo.next();
                ByteMessage message = fifo.get();
                verifyByteMessage(message, 7, 29, null);

                fifo.next();
                message = fifo.get();
                verifyByteMessage(message, 8, 29, null);
            }
        }

    }

    public static class DbIndexTest {

        @SuppressWarnings("unchecked")
        @Test
        @SneakyThrows
        public void addSeqTest() {
            DbIndex a = new DbIndex();
            a.addSeq(10);

            List<Range> list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertEquals(new Range(10, 10), list.get(0));

            a.addSeq(9);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertEquals(new Range(10, 10), list.get(0));

            a.addSeq(10);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertEquals(new Range(10, 10), list.get(0));

            IntStream.range(11, 21)
                .forEach(a::addSeq);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertEquals(new Range(10, 20), list.get(0));

            IntStream.range(30, 41)
                .forEach(a::addSeq);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(2, list.size());
            assertEquals(new Range(10, 20), list.get(0));
            assertEquals(new Range(30, 40), list.get(1));

            a.addSeq(9);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(2, list.size());
            assertEquals(new Range(10, 20), list.get(0));
            assertEquals(new Range(30, 40), list.get(1));

            a.addSeq(25);
            list = (List<Range>) readField(a, "list", true);
            assertEquals(2, list.size());
            assertEquals(new Range(10, 20), list.get(0));
            assertEquals(new Range(30, 40), list.get(1));
        }

        @Test
        @SneakyThrows
        public void seekTest() {
            DbIndex a = new DbIndex();
            assertFalse(a.seek(10));
            assertEquals(0L, a.currentSeq());


            writeField(a, "list", ImmutableList.of(new Range(10, 20)), true);
            assertTrue(a.seek(9));
            assertEquals(10L, a.currentSeq());

            assertTrue(a.seek(20));
            assertEquals(20L, a.currentSeq());

            assertFalse(a.seek(21));
            assertEquals(0L, a.currentSeq());


            writeField(a, "list", asList(new Range(10, 20), new Range(30, 40)), true);
            assertTrue(a.seek(9));
            assertEquals(10L, a.currentSeq());

            assertTrue(a.seek(20));
            assertEquals(20L, a.currentSeq());

            assertTrue(a.seek(25));
            assertEquals(30L, a.currentSeq());

            assertTrue(a.seek(40));
            assertEquals(40L, a.currentSeq());

            assertFalse(a.seek(45));
            assertEquals(0L, a.currentSeq());
        }

        @Test
        @SneakyThrows
        public void nextTest() {
            DbIndex a = new DbIndex();
            assertFalse(a.next());
            assertEquals(0L, a.currentSeq());

            writeField(a, "list", ImmutableList.of(new Range(10, 20)), true);
            assertTrue(a.next());
            assertEquals(10L, a.currentSeq());

            assertTrue(a.next());
            assertEquals(11L, a.currentSeq());

            a.seek(15);

            assertTrue(a.next());
            assertEquals(16L, a.currentSeq());

            for (int i = 0; i < 10; i++) {
                if (!a.next()) {
                    break;
                }
            }
            assertEquals(0L, a.currentSeq());

            a = new DbIndex();
            writeField(a, "list", asList(new Range(10, 20), new Range(30, 40)), true);

            assertTrue(a.seek(20));
            a.next();
            assertEquals(30L, a.currentSeq());

            for (int i = 0; i < 10; i++) {
                a.next();
            }
            assertEquals(40L, a.currentSeq());

            assertFalse(a.next());
        }

        @SuppressWarnings("unchecked")
        @Test
        @SneakyThrows
        public void pruneTest() {
            DbIndex a = new DbIndex();
            LongConsumer count = Mockito.mock(LongConsumer.class);
            a.prune(10, count);
            verify(count, never()).accept(anyLong());

            writeField(a, "list", new ArrayList<Range>() {{
                add(new Range(10, 20));
            }}, true);

            a.prune(9, count);
            verify(count, never()).accept(anyLong());

            a.prune(10, count);
            verify(count, times(1)).accept(10L);

            reset(count);
            a.prune(15, count);
            verify(count, times(5)).accept(anyLong());
            List<Range> list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertThat(list, hasItems(new Range(16, 20)));

            reset(count);
            a.prune(20, count);
            verify(count, times(5)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertEquals(0, list.size());


            a = new DbIndex();
            reset(count);
            writeField(a, "list", new ArrayList<Range>() {{
                add(new Range(10, 20));
                add(new Range(30, 40));
            }}, true);

            a.prune(25, count);
            verify(count, times(11)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertThat(list, hasItems(new Range(30, 40)));

            reset(count);
            a.prune(30, count);
            verify(count, times(1)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertEquals(1, list.size());
            assertThat(list, hasItems(new Range(31, 40)));

            reset(count);
            a.prune(35, count);
            verify(count, times(5)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertThat(list, hasItems(new Range(36, 40)));

            reset(count);
            a.prune(40, count);
            verify(count, times(5)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertEquals(0, list.size());

            a = new DbIndex();
            writeField(a, "list", new ArrayList<Range>() {{
                add(new Range(10, 20));
                add(new Range(30, 40));
            }}, true);

            reset(count);
            a.prune(45, count);
            verify(count, times(22)).accept(anyLong());
            list = (List<Range>) readField(a, "list", true);
            assertEquals(0, list.size());
        }
    }

    public static class RangeTest {

        @SneakyThrows
        private void assertEqualsRange(Range range, long from, long to) {
            assertEquals(from, readField(range, "from", true));
            assertEquals(to, readField(range, "to", true));
        }

        @Test
        public void compareToTest() {
            Range a = new Range(1, 1);
            Range b = new Range(2, 2);
            assertEquals(-1, a.compareTo(b));

            a = new Range(1, 10);
            b = new Range(1, 10);
            assertEquals(0, a.compareTo(b));

            a = new Range(100, 1000);
            b = new Range(10, 90);
            assertEquals(1, a.compareTo(b));
        }

        @Test
        public void inRangeTest() {
            Range a = new Range(10, 10);
            assertFalse(a.inRange(9));
            assertTrue(a.inRange(10));
            assertFalse(a.inRange(11));

            Range b = new Range(10, 100);
            assertFalse(b.inRange(9));
            assertTrue(b.inRange(10));
            assertTrue(b.inRange(11));
            assertTrue(b.inRange(100));
            assertFalse(b.inRange(101));
        }

        @Test
        @SneakyThrows
        public void setNextTest() {
            Range a = new Range(10, 10);
            assertFalse(a.setNext(9));
            assertFalse(a.setNext(10));
            assertTrue(a.setNext(11));
            assertEqualsRange(a, 10, 11);

            a = new Range(10, 10);
            assertFalse(a.setNext(12));

            Range b = new Range(10, 100);
            assertFalse(b.setNext(100));
            assertTrue(b.setNext(101));
            assertEqualsRange(b, 10, 101);

            b = new Range(10, 100);
            assertFalse(b.setNext(102));
        }

        @Test
        public void pruneTest() {
            Range a = new Range(10, 10);
            asList(9, 10, 11)
                .forEach(id -> {
                    a.prune(id);
                    assertEqualsRange(a, 10, 10);
                });

            Range b = new Range(10, 100);
            asList(9, 100, 101)
                .forEach(id -> {
                    b.prune(id);
                    assertEqualsRange(b, 10, 100);
                });

            b.prune(10);
            assertEqualsRange(b, 11, 100);

            b.prune(20);
            assertEqualsRange(b, 21, 100);
        }
    }

}
