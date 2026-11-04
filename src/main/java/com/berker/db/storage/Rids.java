package com.berker.db.storage;

/**
 * Packs a record ID {@code (pageNo, slot)} into one {@code long}: pageNo in the high 32 bits, slot
 * in the low 32. A {@code long} needs no object, so passing record IDs around allocates nothing.
 *
 * <p>Deliberately separate from {@link PageKeys}. Both pack two ints into a long, but a record ID
 * has no fileId, because the table that owns the record already knows its file. Mixing them up
 * would compile fine and then quietly look up the wrong thing.
 *
 * <p>A record ID stays valid while its tuple moves inside the page: it names the slot, and the
 * slot holds the current offset.
 */
public final class Rids {

    private Rids() {
    }

    public static long pack(int pageNo, int slot) {
        // Mask slot: a plain (long) cast would sign-extend a negative int over the pageNo half.
        return ((long) pageNo << 32) | (slot & 0xFFFF_FFFFL);
    }

    public static int pageNo(long rid) {
        return (int) (rid >>> 32);
    }

    public static int slot(long rid) {
        return (int) rid;
    }
}
