package com.zmh.fastlog.worker.file.rock;

import com.google.common.annotations.VisibleForTesting;
import com.zmh.fastlog.model.message.ByteData;
import com.zmh.fastlog.utils.InstanceUtils;
import lombok.*;
import lombok.experimental.Delegate;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.pulsar.shade.org.apache.commons.io.FileSystemUtils;
import org.rocksdb.*;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import static com.zmh.fastlog.utils.BufferUtils.marginToBuffer;
import static com.zmh.fastlog.utils.BufferUtils.writeLongToBufferBE;
import static com.zmh.fastlog.utils.ExceptionUtil.sneakyCatch;
import static com.zmh.fastlog.utils.InstanceUtils.safeClose;
import static com.zmh.fastlog.utils.Utils.sneakyInvoke;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

public class FIFOFile implements Closeable {
    private static String basePath = "logs/cache/rocksdb/";
    private static File baseDir = new File(basePath);

    static {
        RocksDB.loadLibrary();
        //noinspection ResultOfMethodCallIgnored
        baseDir.mkdirs();
    }

    private RandomAccessFile indexFile;
    private DbIndex dbIndex = new DbIndex();


    private DBOptions dbOptions = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundCompactions(4)
        .setAllowMmapReads(false)
        .setDbWriteBufferSize(64 * 1024 * 1024)
        .setNewTableReaderForCompactionInputs(true)
        .setMaxBackgroundCompactions(4)
        .setUseFsync(false);

    private WriteOptions writeOptions = new WriteOptions()
        .setDisableWAL(true)
        .setSync(false);
    private WriteOptionsWrapper writeOptionsWrapper = new WriteOptionsWrapper(writeOptions);
    private ColumnFamilyHandle logColumn = null;
    private ColumnFamilyHandleWrapper logColumnWrapper;
    private RockDbWrapper db;
    private final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

    @VisibleForTesting
    static void cleanUp() {
        //FileSystemUtils.deleteRecursively(baseDir);
        //noinspection ResultOfMethodCallIgnored
        baseDir.mkdirs();
    }

    @SneakyThrows
    private void initRocksDb() {
        ColumnFamilyDescriptor logColumn = new ColumnFamilyDescriptor("log".getBytes());
        logColumn.columnFamilyOptions()
            .setCompressionType(LZ4_COMPRESSION)
            .setTargetFileSizeBase(32 * 1024 * 1024)
            .setMinWriteBufferNumberToMerge(4)
            .setMaxWriteBufferNumber(10)
            .setLevel0FileNumCompactionTrigger(8)
            .setLevel0SlowdownWritesTrigger(17)
            .setLevel0StopWritesTrigger(24)
            .setMaxBytesForLevelBase(64 * 1024 * 1024)
            .setMaxBytesForLevelMultiplier(8)
        ;

        List<ColumnFamilyDescriptor> cfNames = asList(
            new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY),
            logColumn
        );
        List<Integer> ttlValues = asList(
            0,
            30 * 24 * 3600 // 30 days
        );

        db = new RockDbWrapper(TtlDB.open(dbOptions, basePath, cfNames, columnFamilyHandleList, ttlValues, false));
        this.logColumn = columnFamilyHandleList.get(1);
        this.logColumnWrapper = new ColumnFamilyHandleWrapper(this.logColumn);
    }

    @SneakyThrows
    public FIFOFile() {
        indexFile = new RandomAccessFile(new File(basePath + "id-index"), "rwd");
        dbIndex.load(indexFile);
        initRocksDb();
    }

    private long lastSavedTime = currentTimeMillis();
    private long unSaveIndexCount = 0;

    private void delaySave() {
        if (unSaveIndexCount++ > 10000 || currentTimeMillis() - lastSavedTime >= 1000) {
            saveIndex();
        }
    }

    private int saveIndex() {
        unSaveIndexCount = 0;
        lastSavedTime = currentTimeMillis();
        return dbIndex.save(indexFile);
    }


    private final byte[] keyBuffer = new byte[8];

    public void addItem(long id, byte[] data, int len) {
        writeLongToBufferBE(id, keyBuffer, 0);
        try {
            db.put(logColumnWrapper, writeOptionsWrapper, keyBuffer, data, len);
            dbIndex.addSeq(id);
            delaySave();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private ByteData currentMessage;

    public ByteData get() {
        return currentMessage;
    }

    private long lastReadId = 0;

    private byte[] readBuffer = new byte[5120];

    public void next() {
        try {
            if (dbIndex.seek(lastReadId + 1)) {
                do {
                    long seq = dbIndex.currentSeq();
                    lastReadId = seq;
                    writeLongToBufferBE(seq, keyBuffer, 0);
                    int readCount = db.get(logColumn, keyBuffer, readBuffer);
                    if (RocksDB.NOT_FOUND == readCount) {
                        continue;
                    }
                    if (readCount > readBuffer.length) {
                        readBuffer = new byte[marginToBuffer(readCount)];
                        readCount = db.get(logColumn, keyBuffer, readBuffer);
                    }
                    currentMessage = new ByteData(seq, readBuffer, readCount);
                    return;
                } while (dbIndex.next());
            }

        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }
        currentMessage = null;
    }

    public void deleteBeforeId(long seq) {
        dbIndex.prune(seq, this::delete);
        if (seq >= lastReadId) {
            currentMessage = null;
        }
        delaySave();
    }

    private void delete(long id) {
        writeLongToBufferBE(id, keyBuffer, 0);
        sneakyInvoke(() -> db.delete(logColumn, writeOptions, keyBuffer));
    }

    @Override
    public void close() {
        try {
            System.out.println("closing db..");
            int length = saveIndex();
            indexFile.setLength(length);
            indexFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (nonNull(db)) {
/*            if (nonNull(logColumn)) {
                try {
                    db.compactRange(logColumn);
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }*/
            columnFamilyHandleList.forEach(InstanceUtils::safeClose);
            safeClose(db);
        }
        safeClose(logColumn);
        safeClose(dbOptions);
        safeClose(writeOptions);
    }

}

class WriteOptionsWrapper extends WriteOptions {
    @Getter
    private final long nativeHandle;

    @SneakyThrows
    WriteOptionsWrapper(WriteOptions options) {
        nativeHandle = (long) readField(options, "nativeHandle_", true);
    }
}

class ColumnFamilyHandleWrapper {
    @Getter
    private final long nativeHandle;

    @SneakyThrows
    ColumnFamilyHandleWrapper(ColumnFamilyHandle handle) {
        nativeHandle = (long) readField(handle, "nativeHandle_", true);
    }
}

class RockDbWrapper extends TtlDB implements AutoCloseable {
    @Delegate
    private final TtlDB db;

    @Getter
    private final long nativeHandle;

    @SneakyThrows
    protected RockDbWrapper(TtlDB db) {
        super(readNativeHandle(db));
        this.db = db;
        nativeHandle = readNativeHandle(db);
    }

    @SneakyThrows
    public static long readNativeHandle(TtlDB db) {
        return (long) readField(db, "nativeHandle_", true);
    }

    public void put(ColumnFamilyHandleWrapper columnFamilyHandle, WriteOptionsWrapper writeOpts, byte[] key, byte[] value, int len) throws RocksDBException {
        put(nativeHandle, writeOpts.getNativeHandle(), key, 0, key.length, value,
            0, len, columnFamilyHandle.getNativeHandle());
    }
}

@SuppressWarnings("WeakerAccess")
class DbIndex {
    @ToString
    @AllArgsConstructor
    @EqualsAndHashCode
    static class Range implements Comparable<Range> {
        private long from;
        private long to;

        @Override
        public int compareTo(@NotNull Range o) {
            long r = from - o.from;
            if (0 == r) {
                return 0;
            }
            return r > 0 ? 1 : -1;
        }

        public boolean inRange(long seq) {
            return seq >= from && seq <= to;
        }

        public boolean setNext(long seq) {
            if (seq == to + 1) {
                to = seq;
                return true;
            }
            return false;
        }

        public void prune(long seq) {
            if (to > seq && from <= seq) {
                from = seq + 1;
            }
        }
    }

    private long currentSeq;
    private Range currentRange;
    private List<Range> list = new ArrayList<>();

    public void addSeq(long seq) {
        if (list.size() == 0) {
            return;
        }
        Range last = list.get(list.size() - 1);

        if (isNull(last)) {
            list.add(new Range(seq, seq));
            return;
        }
        if (last.to >= seq) {
            return;
        }
        if (!last.setNext(seq)) {
            list.add(new Range(seq, seq));
        }
    }

    public boolean seek(long seq) {
        if (nonNull(currentRange) && currentRange.inRange(seq)) {
            currentSeq = seq;
            return true;
        }
        for (int i = 0; i < list.size(); i++) {
            Range range = list.get(i);
            if (range.inRange(seq)) {
                currentSeq = seq;
                currentRange = range;
                return true;
            }
            if (range.from > seq) {
                currentSeq = range.from;
                currentRange = range;
                return true;
            }
        }
        currentSeq = 0;
        currentRange = null;
        return false;
    }

    public boolean next() {
        return seek(currentSeq + 1);
    }

    public long currentSeq() {
        return currentSeq;
    }

    // delete seq(include) and before seqs
    public void prune(long seq, LongConsumer callback) {
        for (int index = 0; index < list.size(); index++) {

            Range range = list.get(index);
            for (long i = range.from; i <= seq && i <= range.to; i++) {
                callback.accept(i);
            }
            if (seq >= range.to) {
                list.remove(index--);
                if (range.equals(currentRange)) {
                    currentRange = null;
                }
            } else {
                range.prune(seq);
                break;
            }
        }
        if (currentSeq <= seq) {
            seek(seq + 1);
        }
    }

    public int save(RandomAccessFile indexFile) {
        int expectLength = 4 + (8 + 8) * list.size(); // count, [from, to] * count
        try {
            indexFile.seek(0);
            indexFile.writeInt(list.size());
            list.forEach(sneakyCatch(r -> {
                indexFile.writeLong(r.from);
                indexFile.writeLong(r.to);
            }));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return expectLength;
    }

    public void load(RandomAccessFile file) {
        try {
            if (file.length() < 20) {
                return;
            }
            file.seek(0);
            int count = file.readInt();
            list.clear();
            for (int i = 0; i < count; i++) {
                long from = file.readLong();
                long to = file.readLong();
                list.add(new Range(from, to));
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}