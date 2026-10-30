package com.berker.db.storage;

/**
 * Packs a page's identity {@code (fileId, pageNo)} into one {@code long}: fileId in the high
 * 32 bits, pageNo in the low 32. The BufferPool uses it as its lookup key, because a
 * {@code long} needs no object, while a {@code (fileId, pageNo)} record would be one object
 * per lookup.
 *
 * <p>This is deliberately a separate class from the record-ID (RID) helpers that will come
 * with TableHeap. Both pack two ints into a long, so mixing them up would compile fine and
 * then quietly look up the wrong thing.
 *
 * <p>fileIds are at most {@link DiskManager#MAX_FILE_ID} (16 bits), so the top bit of a key
 * is always 0: every valid key is non-negative. Code that needs an "empty" marker can use
 * {@code -1} without colliding with a real page.
 */
public final class PageKeys {

    private PageKeys() {
    }

    public static long pack(int fileId, int pageNo) {
        // Mask pageNo: a plain (long) cast would sign-extend a negative int and smear 1-bits
        // over the fileId half. Page numbers are never negative, but the key must not lie
        // if one ever is.
        return ((long) fileId << 32) | (pageNo & 0xFFFF_FFFFL);
    }

    public static int fileId(long key) {
        return (int) (key >>> 32);
    }

    public static int pageNo(long key) {
        return (int) key;
    }
}
