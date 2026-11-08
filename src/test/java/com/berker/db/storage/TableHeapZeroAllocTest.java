package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the row paths allocate nothing: writing rows, inserting, reading by record ID, updating
 * within a page, deleting, and a full scan that reads every column. This also covers
 * {@link TupleLayout}'s accessors and
 * {@link TupleView}, including {@code MemorySegment.mismatch} in the VARCHAR comparison. Same
 * method as the other zero-alloc tests, warmed up by repeating the measured call.
 *
 * <p>Inserts and deletes are balanced, so no pages are added here: growing the free-space map is
 * the one allocation in these paths, and it happens each time a table doubles, not per row.
 */
@Tag("zero-alloc")
class TableHeapZeroAllocTest {

    private static final int FILE = 0;
    private static final int ID = 0;
    private static final int NAME = 1;
    private static final int NAME_LENGTH = 40;
    private static final int ROWS = 2_000;
    private static final int WARMUP_ROUNDS = 4;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static final TupleLayout LAYOUT = new TupleLayout(new Schema(List.of(
            new Column("id", ColumnType.BIGINT, false),
            Column.varchar("name", 100, true),
            new Column("age", ColumnType.INT, true))));

    @TempDir
    Path dir;

    private Arena arena;
    private MemorySegment name;
    private MemorySegment readBuffer;
    private TupleWriter writer;
    private TupleView view;
    private DiskManager disk;
    private BufferPool pool;
    private TableHeap heap;
    private final HeapContext context = new HeapContext();
    private long sink;

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        name = arena.allocate(NAME_LENGTH);
        name.fill((byte) 'n');
        readBuffer = arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8);
        writer = new TupleWriter(LAYOUT, arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8));
        view = new TupleView(LAYOUT);
        disk = new DiskManager();
        disk.openFile(FILE, dir.resolve("table"));
        pool = new BufferPool(disk, 16);
        heap = new TableHeap(pool, disk, FILE);
        for (int i = 0; i < ROWS; i++) {
            insert(i);
        }
    }

    @AfterEach
    void tearDown() {
        pool.close();
        disk.close();
        arena.close();
    }

    private long insert(long id) {
        writer.begin();
        writer.setLong(ID, id);
        writer.setVarchar(NAME, name, 0, NAME_LENGTH);
        writer.setInt(2, (int) id);
        return heap.insert(context, writer.segment(), 0, writer.finish());
    }

    @Test
    void insertReadDeleteAllocateNothing() {
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            churn(20_000);
        }
        long before = THREADS.getCurrentThreadAllocatedBytes();
        churn(20_000);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by insert/read/delete rounds");
    }

    @Test
    void fullScansAllocateNothing() {
        try (TableScan scan = new TableScan(heap, LAYOUT)) {
            for (int i = 0; i < WARMUP_ROUNDS; i++) {
                scan(scan, 20);
            }
            long before = THREADS.getCurrentThreadAllocatedBytes();
            scan(scan, 20);
            long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
            assertEquals(0, allocated, "bytes allocated by full scans");
        }
    }

    private void churn(int iterations) {
        for (int i = 0; i < iterations; i++) {
            long rid = insert(i);
            int length = heap.read(context, rid, readBuffer, 0);
            sink += view.wrap(readBuffer, 0, length).getLong(ID);
            writer.begin();
            writer.setLong(ID, i);
            writer.setVarchar(NAME, name, 0, (i & 1) == 0 ? 8 : NAME_LENGTH); // shrinks or stays
            rid = heap.update(context, rid, writer.segment(), 0, writer.finish());
            heap.delete(context, rid);
        }
    }

    private void scan(TableScan scan, int rounds) {
        for (int round = 0; round < rounds; round++) {
            scan.rewind();
            while (scan.next()) {
                TupleView tuple = scan.tuple();
                sink += tuple.getLong(ID) + tuple.getInt(2);
                if (!tuple.isNull(NAME)) {
                    sink += tuple.compareVarchar(NAME, name, 0, NAME_LENGTH);
                }
            }
        }
    }
}
