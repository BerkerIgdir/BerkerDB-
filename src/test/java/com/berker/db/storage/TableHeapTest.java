package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableHeapTest {

    private static final int FILE = 3;
    private static final int ID = 0;
    private static final int NAME = 1;
    private static final TupleLayout LAYOUT = new TupleLayout(new Schema(List.of(
            new Column("id", ColumnType.BIGINT, false),
            Column.varchar("name", 8000, true))));

    @TempDir
    Path dir;

    private Arena arena;
    private MemorySegment rowBuffer;
    private MemorySegment readBuffer;
    private MemorySegment name;
    private TupleWriter writer;
    private TupleView view;
    private DiskManager disk;
    private BufferPool pool;
    private TableHeap heap;
    private final HeapContext context = new HeapContext();

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        rowBuffer = arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8);
        readBuffer = arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8);
        name = arena.allocate(8000);
        name.fill((byte) 'n');
        writer = new TupleWriter(LAYOUT, rowBuffer);
        view = new TupleView(LAYOUT);
        disk = new DiskManager();
        disk.openFile(FILE, dir.resolve("table"));
        pool = new BufferPool(disk, 32);
        heap = new TableHeap(pool, disk, FILE);
    }

    @AfterEach
    void tearDown() {
        pool.close();
        disk.close();
        arena.close();
    }

    private long insert(long id, int nameLength) {
        return insert(context, writer, id, nameLength);
    }

    private long insert(HeapContext ctx, TupleWriter w, long id, int nameLength) {
        w.begin();
        w.setLong(ID, id);
        w.setVarchar(NAME, name, 0, nameLength);
        return heap.insert(ctx, w.segment(), 0, w.finish());
    }

    /** The id of the row at {@code rid}. */
    private long idAt(long rid) {
        int length = heap.read(context, rid, readBuffer, 0);
        assertTrue(length > 0, "no row at page " + Rids.pageNo(rid) + " slot " + Rids.slot(rid));
        return view.wrap(readBuffer, 0, length).getLong(ID);
    }

    private Map<Long, Long> scanAll() {
        Map<Long, Long> ids = new HashMap<>();
        try (TableScan scan = new TableScan(heap, LAYOUT)) {
            while (scan.next()) {
                assertEquals(null, ids.put(scan.rid(), scan.tuple().getLong(ID)), "row seen twice");
            }
        }
        return ids;
    }

    @Test
    void aNewHeapIsEmpty() {
        assertEquals(0, heap.pageCount());
        assertEquals(0, scanAll().size());
    }

    @Test
    void insertedRowsCanBeReadBackByRid() {
        long a = insert(1, 10);
        long b = insert(2, 20);

        assertEquals(1, idAt(a));
        assertEquals(2, idAt(b));
        assertEquals(0, Rids.pageNo(a));
        assertEquals(1, heap.pageCount());
    }

    @Test
    void varcharBytesSurviveTheRoundTrip() {
        long rid = insert(1, 300);

        heap.read(context, rid, readBuffer, 0);
        view.wrap(readBuffer, 0, LAYOUT.varDataOffset() + 300);

        assertEquals(0, view.compareVarchar(NAME, name, 0, 300));
    }

    @Test
    void aFullPageSpillsToANewOne() {
        long first = insert(0, 4000);
        long second = insert(1, 4000);
        long third = insert(2, 4000);

        assertEquals(0, Rids.pageNo(first));
        assertEquals(0, Rids.pageNo(second));
        assertEquals(1, Rids.pageNo(third));
        assertEquals(2, heap.pageCount());
    }

    @Test
    void spaceFreedByADeleteIsUsedBeforeAddingPages() {
        long first = insert(0, 7000);
        insert(1, 7000);
        assertEquals(2, heap.pageCount());

        assertTrue(heap.delete(context, first));
        long reused = insert(2, 7000);

        assertEquals(0, Rids.pageNo(reused));
        assertEquals(2, heap.pageCount());
    }

    @Test
    void insertsFillPagesInsteadOfLeavingThemHalfEmpty() {
        for (int i = 0; i < 5_000; i++) {
            insert(i, 40);
        }

        // A row is 17 + 40 bytes, rounded up to 64, plus a 4-byte slot: 120 fit on a page.
        assertEquals(5_000 / 120 + 1, heap.pageCount());
    }

    @Test
    void deletedRowsAreGoneAndOthersStayFindable() {
        long a = insert(1, 10);
        long b = insert(2, 10);
        long c = insert(3, 10);

        assertTrue(heap.delete(context, b));

        assertEquals(1, idAt(a));
        assertEquals(3, idAt(c));
        assertEquals(-1, heap.read(context, b, readBuffer, 0));
        assertFalse(heap.delete(context, b));
        assertEquals(Map.of(a, 1L, c, 3L), scanAll());
    }

    /** Builds a row in {@code writer} and returns its length. */
    private int row(long id, int nameLength) {
        writer.begin();
        writer.setLong(ID, id);
        writer.setVarchar(NAME, name, 0, nameLength);
        return writer.finish();
    }

    private long update(long rid, long id, int nameLength) {
        return heap.update(context, rid, rowBuffer, 0, row(id, nameLength));
    }

    @Test
    void anUpdateThatFitsKeepsTheRid() {
        long a = insert(1, 10);
        long b = insert(2, 10);

        assertEquals(a, update(a, 10, 3000));

        assertEquals(10, idAt(a));
        assertEquals(2, idAt(b));
        assertEquals(1, heap.pageCount());
    }

    @Test
    void anUpdateThatDoesNotFitMovesTheRowAndGivesANewRid() {
        long a = insert(1, 4000);
        long b = insert(2, 4000);

        long moved = update(a, 10, 7000);

        assertTrue(moved != a);
        assertEquals(1, Rids.pageNo(moved));
        assertEquals(10, idAt(moved));
        assertEquals(-1, heap.read(context, a, readBuffer, 0), "the old version is gone");
        assertEquals(2, idAt(b));
        assertEquals(Map.of(b, 2L, moved, 10L), scanAll());
    }

    @Test
    void updatesOfMissingRowsReturnMinusOne() {
        long a = insert(1, 10);
        heap.delete(context, a);

        assertEquals(-1, update(a, 2, 10));
        assertEquals(-1, update(Rids.pack(9, 0), 2, 10));
        assertEquals(-1, update(Rids.pack(0, 99), 2, 10));
        assertEquals(0, scanAll().size());
        assertThrows(IllegalArgumentException.class, () -> heap.update(context, a, rowBuffer, 0, 0));
    }

    @Test
    void ridsOfMissingRowsAreAnsweredNotThrown() {
        insert(1, 10);

        assertEquals(-1, heap.read(context, Rids.pack(0, 99), readBuffer, 0));
        assertEquals(-1, heap.read(context, Rids.pack(0, -1), readBuffer, 0));
        assertEquals(-1, heap.read(context, Rids.pack(5, 0), readBuffer, 0));
        assertEquals(-1, heap.read(context, Rids.pack(-1, 0), readBuffer, 0));
        assertFalse(heap.delete(context, Rids.pack(0, 99)));
        assertFalse(heap.delete(context, Rids.pack(5, 0)));
    }

    @Test
    void rowLengthsOutsideThePageLimitAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> heap.insert(context, rowBuffer, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> heap.insert(context, rowBuffer, 0, SlottedPage.MAX_TUPLE_SIZE + 1));
        assertEquals(0, heap.pageCount());
    }

    @Test
    void theLargestRowFits() {
        long rid = heap.insert(context, rowBuffer, 0, SlottedPage.MAX_TUPLE_SIZE);

        assertEquals(SlottedPage.MAX_TUPLE_SIZE, heap.read(context, rid, readBuffer, 0));
    }

    @Test
    void aScanSeesEveryRowAcrossPagesEvenWithSmallBufferPool() {
        Map<Long, Long> expected = new HashMap<>();
        for (int i = 0; i < 20_000; i++) {
            expected.put(insert(i, 100), (long) i);
        }

        assertTrue(heap.pageCount() > 32, "the table must be bigger than the pool");
        assertEquals(expected, scanAll());
    }

    @Test
    void aScanCanBeRewound() {
        insert(1, 10);
        insert(2, 10);
        try (TableScan scan = new TableScan(heap, LAYOUT)) {
            while (scan.next()) {
                // run to the end
            }
            assertFalse(scan.next());

            scan.rewind();

            assertTrue(scan.next());
            assertEquals(1, scan.tuple().getLong(ID));
        }
    }

    @Test
    void aScanHoldsNoLatchSoItsOwnThreadCanChangeTheTable() {
        long a = insert(1, 10);
        long b = insert(2, 10);
        try (TableScan scan = new TableScan(heap, LAYOUT)) {
            assertTrue(scan.next());

            // Would deadlock if the scan kept its READ latch on the page.
            assertTrue(heap.delete(context, b));
            insert(3, 10);

            // The scan goes on over its copy of the page.
            assertEquals(1, scan.tuple().getLong(ID));
            assertEquals(a, scan.rid());
            assertTrue(scan.next());
            assertEquals(2, scan.tuple().getLong(ID));
        }
    }

    @Test
    void rowsAndFreeSpaceSurviveReopening() {
        Map<Long, Long> expected = new HashMap<>();
        for (int i = 0; i < 5_000; i++) {
            expected.put(insert(i, 200), (long) i);
        }
        int pages = heap.pageCount();
        long toDelete = expected.keySet().iterator().next();
        heap.delete(context, toDelete);
        expected.remove(toDelete);
        pool.flushAll();
        pool.close();

        pool = new BufferPool(disk, 32);
        heap = new TableHeap(pool, disk, FILE);

        assertEquals(pages, heap.pageCount());
        assertEquals(expected, scanAll());
        insert(9_999, 1); // goes into free space the rebuilt map knows about
        assertEquals(pages, heap.pageCount());
    }

    @Test
    void opensAPageThatWasAllocatedButNeverWritten() {
        long first = insert(1, 10);
        pool.flushAll();
        pool.close();
        disk.allocatePage(FILE);

        pool = new BufferPool(disk, 32);
        heap = new TableHeap(pool, disk, FILE);

        assertEquals(2, heap.pageCount());
        assertEquals(0, Rids.pageNo(insert(2, 7000)));
        assertEquals(1, Rids.pageNo(insert(3, 7000))); // the blank page is usable, not skipped
        assertEquals(2, heap.pageCount());
        assertEquals(1, idAt(first));
    }

    @Test
    void randomInsertsAndDeletesMatchAModel() {
        Random random = new Random(42);
        Map<Long, Long> model = new HashMap<>();
        List<Long> rids = new ArrayList<>();
        long nextId = 0;
        for (int step = 0; step < 30_000; step++) {
            int action = rids.isEmpty() ? 0 : random.nextInt(100);
            if (action >= 45 && action < 60) {
                int index = random.nextInt(rids.size());
                long rid = rids.get(index);
                long id = nextId++;
                long newRid = update(rid, id, random.nextInt(4000));
                assertTrue(newRid != -1);
                model.remove(rid);
                assertEquals(null, model.put(newRid, id), "rid handed out twice");
                rids.set(index, newRid);
            } else if (action < 45) {
                long id = nextId++;
                long rid = insert(id, random.nextInt(1000));
                assertEquals(null, model.put(rid, id), "rid handed out twice");
                rids.add(rid);
            } else {
                int index = random.nextInt(rids.size());
                long rid = rids.get(index);
                rids.set(index, rids.getLast());
                rids.removeLast();
                assertEquals(model.remove(rid), idAt(rid));
                assertTrue(heap.delete(context, rid));
            }
        }
        assertEquals(model, scanAll());
    }

    @Test
    void concurrentInsertsAndDeletesKeepEveryRow() throws Exception {
        int threads = 4;
        int rowsPerThread = 3_000;
        Set<Long> survivors = ConcurrentHashMap.newKeySet();
        List<Throwable> failures = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            Thread worker = new Thread(() -> {
                HeapContext ctx = new HeapContext();
                TupleWriter w = new TupleWriter(LAYOUT, arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8));
                Random random = new Random(thread);
                for (int i = 0; i < rowsPerThread; i++) {
                    long id = thread * 1_000_000L + i;
                    long rid = insert(ctx, w, id, random.nextInt(500));
                    if ((i & 3) == 0) {
                        assertTrue(heap.delete(ctx, rid));
                    } else {
                        survivors.add(id);
                    }
                }
            });
            worker.setUncaughtExceptionHandler((x, e) -> {
                synchronized (failures) {
                    failures.add(e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertEquals(List.of(), failures);
        assertEquals(survivors, new HashSet<>(scanAll().values()));
        assertEquals(survivors.size(), scanAll().size());
    }
}
