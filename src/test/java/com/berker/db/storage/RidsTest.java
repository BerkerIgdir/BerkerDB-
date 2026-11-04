package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RidsTest {

    @Test
    void packedValuesComeBack() {
        long rid = Rids.pack(7, 3);

        assertEquals(7, Rids.pageNo(rid));
        assertEquals(3, Rids.slot(rid));
    }

    @Test
    void extremeValuesDoNotSpillIntoEachOther() {
        long rid = Rids.pack(Integer.MAX_VALUE, -1);

        assertEquals(Integer.MAX_VALUE, Rids.pageNo(rid));
        assertEquals(-1, Rids.slot(rid));
    }

    @Test
    void pageNoIsTheHighHalf() {
        assertEquals(0x0000_0002_0000_0001L, Rids.pack(2, 1));
    }
}
