package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LruReplacerTest {

    @Test
    void emptyHasNoVictim() {
        LruReplacer lru = new LruReplacer(4);
        assertEquals(-1, lru.pollVictim());
        assertEquals(0, lru.size());
    }

    @Test
    void victimsComeOutLeastRecentlyUsedFirst() {
        LruReplacer lru = new LruReplacer(4);
        lru.add(2);
        lru.add(0);
        lru.add(3);
        assertEquals(3, lru.size());
        assertEquals(2, lru.pollVictim());
        assertEquals(0, lru.pollVictim());
        assertEquals(3, lru.pollVictim());
        assertEquals(-1, lru.pollVictim());
    }

    @Test
    void addingAgainMakesAFrameMostRecent() {
        LruReplacer lru = new LruReplacer(4);
        lru.add(0);
        lru.add(1);
        lru.add(2);
        lru.add(0); // used again
        assertEquals(3, lru.size(), "not listed twice");
        assertEquals(1, lru.pollVictim());
        assertEquals(2, lru.pollVictim());
        assertEquals(0, lru.pollVictim());
    }

    @Test
    void removedFramesAreNeverVictims() {
        LruReplacer lru = new LruReplacer(4);
        lru.add(0);
        lru.add(1);
        lru.add(2);
        lru.remove(1);   // middle
        lru.remove(0);   // head
        lru.remove(3);   // not listed: no-op
        assertFalse(lru.contains(1));
        assertTrue(lru.contains(2));
        assertEquals(1, lru.size());
        assertEquals(2, lru.pollVictim());
        assertEquals(-1, lru.pollVictim());
    }

    @Test
    void listStaysIntactAfterRemovingTheTail() {
        LruReplacer lru = new LruReplacer(4);
        lru.add(0);
        lru.add(1);
        lru.remove(1);   // tail
        lru.add(2);      // must link after 0, not after the removed 1
        assertEquals(0, lru.pollVictim());
        assertEquals(2, lru.pollVictim());
        assertEquals(-1, lru.pollVictim());
    }
}
