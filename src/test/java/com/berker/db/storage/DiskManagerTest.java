package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskManagerTest {

    @TempDir
    Path dir;

    private Arena arena;
    private DiskManager disk;

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        disk = new DiskManager();
    }

    @AfterEach
    void tearDown() {
        disk.close();
        arena.close();
    }

    private MemorySegment page() {
        return arena.allocate(PAGE_SIZE);
    }

    /** Fills the page with a pattern that differs per seed and per position. */
    private static void fill(MemorySegment page, int seed) {
        for (int i = 0; i < PAGE_SIZE; i++) {
            page.set(ValueLayout.JAVA_BYTE, i, (byte) (seed * 31 + i));
        }
    }

    private static void assertFilled(MemorySegment page, int seed) {
        for (int i = 0; i < PAGE_SIZE; i++) {
            assertEquals((byte) (seed * 31 + i), page.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
        }
    }

    // ---------------------------------------------------------------- basics

    @Test
    void newFileHasNoPages() {
        disk.openFile(0, dir.resolve("t0"));
        assertEquals(0, disk.pageCount(0));
    }

    @Test
    void allocateReturnsSequentialPageNumbersAndGrowsFile() throws IOException {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        assertEquals(0, disk.allocatePage(0));
        assertEquals(1, disk.allocatePage(0));
        assertEquals(2, disk.allocatePage(0));
        assertEquals(3, disk.pageCount(0));
        assertEquals(3L * PAGE_SIZE, Files.size(file));
    }

    @Test
    void allocatedPageIsZeroed() {
        disk.openFile(0, dir.resolve("t0"));
        int pageNo = disk.allocatePage(0);
        MemorySegment page = page();
        page.fill((byte) 0x7F);
        disk.readPage(0, pageNo, page);
        for (int i = 0; i < PAGE_SIZE; i++) {
            assertEquals(0, page.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
        }
    }

    @Test
    void writeThenReadRoundTrip() {
        disk.openFile(0, dir.resolve("t0"));
        int pageNo = disk.allocatePage(0);
        MemorySegment out = page();
        fill(out, 42);
        disk.writePage(0, pageNo, out);

        MemorySegment in = page();
        disk.readPage(0, pageNo, in);
        assertFilled(in, 42);
    }

    @Test
    void pagesDoNotOverlap() {
        disk.openFile(0, dir.resolve("t0"));
        MemorySegment buf = page();
        for (int p = 0; p < 5; p++) {
            disk.allocatePage(0);
        }
        // Write out of order so a wrong offset can't line up by accident.
        int[] order = {3, 0, 4, 1, 2};
        for (int p : order) {
            fill(buf, p);
            disk.writePage(0, p, buf);
        }
        for (int p = 0; p < 5; p++) {
            disk.readPage(0, p, buf);
            assertFilled(buf, p);
        }
    }

    @Test
    void dataSurvivesReopen() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        disk.allocatePage(0);
        disk.allocatePage(0);
        MemorySegment buf = page();
        fill(buf, 7);
        disk.writePage(0, 1, buf);
        disk.sync(0);
        disk.close();

        disk = new DiskManager();
        disk.openFile(0, file);
        assertEquals(2, disk.pageCount(0));
        disk.readPage(0, 1, buf);
        assertFilled(buf, 7);
    }

    @Test
    void filesWithDifferentIdsAreIndependent() {
        disk.openFile(0, dir.resolve("a"));
        disk.openFile(5, dir.resolve("b"));
        disk.allocatePage(0);
        disk.allocatePage(5);
        disk.allocatePage(5);

        MemorySegment buf = page();
        fill(buf, 1);
        disk.writePage(0, 0, buf);
        fill(buf, 2);
        disk.writePage(5, 0, buf);

        assertEquals(1, disk.pageCount(0));
        assertEquals(2, disk.pageCount(5));
        disk.readPage(0, 0, buf);
        assertFilled(buf, 1);
        disk.readPage(5, 0, buf);
        assertFilled(buf, 2);
    }

    @Test
    void largestFileIdWorks() {
        disk.openFile(DiskManager.MAX_FILE_ID, dir.resolve("big"));
        assertEquals(0, disk.allocatePage(DiskManager.MAX_FILE_ID));
    }

    @Test
    void byteBufferOverloadsWork() {
        disk.openFile(0, dir.resolve("t0"));
        disk.allocatePage(0);
        MemorySegment seg = page();
        ByteBuffer buf = seg.asByteBuffer();
        fill(seg, 9);
        buf.position(100); // position/limit must not matter
        disk.writePage(0, 0, buf);
        seg.fill((byte) 0);
        disk.readPage(0, 0, buf);
        assertFilled(seg, 9);
    }

    @Test
    void closeFileThenReopenUnderSameId() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        disk.allocatePage(0);
        disk.closeFile(0);
        assertThrows(IllegalArgumentException.class, () -> disk.pageCount(0));
        disk.openFile(0, file);
        assertEquals(1, disk.pageCount(0));
    }

    // ---------------------------------------------------------------- errors

    @Test
    void readOrWriteOutOfRangeThrows() {
        disk.openFile(0, dir.resolve("t0"));
        disk.allocatePage(0);
        MemorySegment buf = page();
        assertThrows(IndexOutOfBoundsException.class, () -> disk.readPage(0, 1, buf));
        assertThrows(IndexOutOfBoundsException.class, () -> disk.readPage(0, -1, buf));
        assertThrows(IndexOutOfBoundsException.class, () -> disk.writePage(0, 1, buf));
    }

    @Test
    void unknownFileIdThrows() {
        MemorySegment buf = page();
        assertThrows(IllegalArgumentException.class, () -> disk.readPage(3, 0, buf));
        assertThrows(IllegalArgumentException.class, () -> disk.allocatePage(-1));
        assertThrows(IllegalArgumentException.class, () -> disk.pageCount(1_000_000));
    }

    @Test
    void fileIdOutOfRangeIsRejectedOnOpen() {
        assertThrows(IllegalArgumentException.class, () -> disk.openFile(-1, dir.resolve("x")));
        assertThrows(IllegalArgumentException.class,
                () -> disk.openFile(DiskManager.MAX_FILE_ID + 1, dir.resolve("x")));
    }

    @Test
    void openingSameIdTwiceThrows() {
        disk.openFile(0, dir.resolve("a"));
        assertThrows(IllegalStateException.class, () -> disk.openFile(0, dir.resolve("b")));
    }

    @Test
    void wrongBufferIsRejected() {
        disk.openFile(0, dir.resolve("t0"));
        disk.allocatePage(0);
        assertThrows(IllegalArgumentException.class,
                () -> disk.readPage(0, 0, ByteBuffer.allocateDirect(PAGE_SIZE - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> disk.readPage(0, 0, ByteBuffer.allocate(PAGE_SIZE))); // heap buffer
        assertThrows(IllegalArgumentException.class,
                () -> disk.writePage(0, 0, arena.allocate(PAGE_SIZE * 2L)));
        assertThrows(IllegalArgumentException.class,
                () -> disk.writePage(0, 0, MemorySegment.ofArray(new byte[PAGE_SIZE]))); // heap segment
    }

    @Test
    void tornFileIsRefused() throws IOException {
        Path file = dir.resolve("torn");
        Files.write(file, new byte[PAGE_SIZE + 100]);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> disk.openFile(0, file));
        assertTrue(e.getMessage().contains("not a multiple"), e.getMessage());
        // The failed open must not leave the id half-registered.
        assertThrows(IllegalArgumentException.class, () -> disk.pageCount(0));
    }

    @Test
    void useAfterCloseThrows() {
        disk.openFile(0, dir.resolve("t0"));
        disk.close();
        assertThrows(IllegalArgumentException.class, () -> disk.allocatePage(0));
        assertThrows(IllegalStateException.class, () -> disk.openFile(1, dir.resolve("t1")));
        disk.close(); // idempotent
    }
}
