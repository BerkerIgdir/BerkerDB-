package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PageKeysTest {

    @Test
    void roundTrip() {
        long key = PageKeys.pack(7, 12345);
        assertEquals(7, PageKeys.fileId(key));
        assertEquals(12345, PageKeys.pageNo(key));
    }

    @Test
    void extremesRoundTrip() {
        long key = PageKeys.pack(DiskManager.MAX_FILE_ID, Integer.MAX_VALUE);
        assertEquals(DiskManager.MAX_FILE_ID, PageKeys.fileId(key));
        assertEquals(Integer.MAX_VALUE, PageKeys.pageNo(key));
    }

    @Test
    void samePageNoInDifferentFilesGivesDifferentKeys() {
        assertNotEquals(PageKeys.pack(0, 5), PageKeys.pack(1, 5));
        assertNotEquals(PageKeys.pack(1, 0), PageKeys.pack(0, 1));
    }

    @Test
    void validKeysAreNeverNegative() {
        // The BufferPool and LongIntHashMap use -1 as "empty"; a real key must never equal it.
        assertTrue(PageKeys.pack(DiskManager.MAX_FILE_ID, Integer.MAX_VALUE) >= 0);
        assertTrue(PageKeys.pack(0, 0) >= 0);
    }

    @Test
    void negativePageNoDoesNotLeakIntoTheFileId() {
        long key = PageKeys.pack(3, -1);
        assertEquals(3, PageKeys.fileId(key));
        assertEquals(-1, PageKeys.pageNo(key));
    }
}
