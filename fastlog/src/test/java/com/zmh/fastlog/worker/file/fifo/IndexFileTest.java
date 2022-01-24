package com.zmh.fastlog.worker.file.fifo;

import com.zmh.fastlog.worker.BeforeDeleteFile;
import com.zmh.fastlog.worker.file.fifo.IndexFile;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class IndexFileTest extends BeforeDeleteFile {

    @Test
    public void testIndexFileWriteAndReadSingleTime() {
        try (IndexFile indexFile = new IndexFile(Paths.get("logs/cache"))) {
            assertEquals(0, indexFile.readIndex(0));
            assertEquals(0, indexFile.writeIndex(0));

            indexFile.write(0, 56);
            assertEquals(56, indexFile.writeIndex(0));

            indexFile.read(0, 8);
            assertEquals(8, indexFile.readIndex(0));
        }
    }

    @Test
    public void testIndexFileOverSize() {
        try (IndexFile indexFile = new IndexFile(Paths.get("logs/cache"))) {
            assertEquals(0, indexFile.readIndex(2));
            assertEquals(0, indexFile.writeIndex(2));

            indexFile.write(1, 24);
            indexFile.read(1, 24);
            assertEquals(24, indexFile.writeIndex(1));
            assertEquals(24, indexFile.readIndex(1));

            indexFile.write(2, 56);
            indexFile.read(2, 8);
            assertEquals(56, indexFile.writeIndex(2));
            assertEquals(8, indexFile.readIndex(2));
        }
    }

    @Test
    public void testIndexFileWriteAndReadManyTime() {
        try (IndexFile indexFile = new IndexFile(Paths.get("logs/cache"))) {
            indexFile.write(1, 24);
            indexFile.write(1, 16);
            assertEquals(40, indexFile.writeIndex(1));

            indexFile.read(1, 8);
            indexFile.read(1, 16);
            assertEquals(24, indexFile.readIndex(1));
        }
    }

    @Test
    public void closeAndReRead() {
        try (IndexFile indexFile = new IndexFile(Paths.get("logs/cache"))) {
            indexFile.write(1, 24);
            indexFile.write(1, 16);
            assertEquals(40, indexFile.writeIndex(1));

            indexFile.read(1, 8);
            indexFile.read(1, 16);
            assertEquals(24, indexFile.readIndex(1));
        }

        try (IndexFile indexFile = new IndexFile(Paths.get("logs/cache"))) {
            assertEquals(40, indexFile.writeIndex(1));
            assertEquals(24, indexFile.readIndex(1));
        }
    }

}
