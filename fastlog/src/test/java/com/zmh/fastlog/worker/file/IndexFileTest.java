package com.zmh.fastlog.worker.file;

import com.zmh.fastlog.worker.BeforeDeleteFile;
import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class IndexFileTest extends BeforeDeleteFile {

    @Test
    @SneakyThrows
    public void writeAndReadTest() {
        try (IndexFile indexFile = new IndexFile(Paths.get(FOLDER))) {
            indexFile.writeIndex(0, 1);
            assertEquals(1, indexFile.writeIndex(0));

            indexFile.readIndex(0, 1);
            assertEquals(1, indexFile.readIndex(0));

            indexFile.reset(0, 2,2);
            assertEquals(2, indexFile.readIndex(0));
            assertEquals(2, indexFile.writeIndex(0));
        }
    }

    @Test
    @SneakyThrows
    public void writeAndReadAfterCloseTest() {
        try (IndexFile indexFile = new IndexFile(Paths.get(FOLDER))) {
            indexFile.reset(0, 2,2);
            assertEquals(2, indexFile.readIndex(0));
            assertEquals(2, indexFile.writeIndex(0));

            indexFile.reset(1, 3,3);
            assertEquals(3, indexFile.readIndex(1));
            assertEquals(3, indexFile.writeIndex(1));
        }

        try (IndexFile indexFile = new IndexFile(Paths.get(FOLDER))) {
            assertEquals(2, indexFile.readIndex(0));
            assertEquals(2, indexFile.writeIndex(0));
            assertEquals(3, indexFile.readIndex(1));
            assertEquals(3, indexFile.writeIndex(1));
        }
    }
}
