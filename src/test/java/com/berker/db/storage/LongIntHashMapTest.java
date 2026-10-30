package com.berker.db.storage;

import static com.berker.db.storage.LongIntHashMap.MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class LongIntHashMapTest {

    @Test
    void putGetRemove() {
        LongIntHashMap map = new LongIntHashMap(4);
        assertEquals(MISSING, map.get(10));
        assertEquals(MISSING, map.put(10, 1));
        assertEquals(1, map.get(10));
        assertEquals(1, map.size());

        assertEquals(1, map.put(10, 2), "put on an existing key returns the old value");
        assertEquals(2, map.get(10));
        assertEquals(1, map.size());

        assertEquals(2, map.remove(10));
        assertEquals(MISSING, map.get(10));
        assertEquals(MISSING, map.remove(10));
        assertEquals(0, map.size());
    }

    @Test
    void refusesMoreThanMaxEntries() {
        LongIntHashMap map = new LongIntHashMap(3);
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        assertThrows(IllegalStateException.class, () -> map.put(4, 4));
        map.put(3, 30); // replacing an existing key is still fine
        assertEquals(30, map.get(3));
    }

    @Test
    void rejectsNegativeKeysAndValues() {
        LongIntHashMap map = new LongIntHashMap(2);
        assertThrows(IllegalArgumentException.class, () -> map.put(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> map.put(0, -1));
    }

    @Test
    void worksAtFullLoadWithPageLikeKeys() {
        int n = 1000;
        LongIntHashMap map = new LongIntHashMap(n);
        for (int i = 0; i < n; i++) {
            map.put(PageKeys.pack(i % 3, i), i);
        }
        for (int i = 0; i < n; i++) {
            assertEquals(i, map.get(PageKeys.pack(i % 3, i)));
        }
        assertEquals(MISSING, map.get(PageKeys.pack(5, 0)));
    }

    /**
     * Compares against HashMap through a long random sequence of puts and removes, with a small
     * key range so chains collide and wrap around the end of the table constantly. This is
     * what checks the backward-shift deletion: one wrong move and some key becomes unfindable.
     */
    @Test
    void matchesHashMapUnderRandomPutsAndRemoves() {
        int maxEntries = 64;
        LongIntHashMap map = new LongIntHashMap(maxEntries);
        Map<Long, Integer> reference = new HashMap<>();
        Random random = new Random(42);

        for (int step = 0; step < 200_000; step++) {
            long key = random.nextInt(maxEntries * 2);
            if (random.nextBoolean() && (reference.size() < maxEntries || reference.containsKey(key))) {
                int value = random.nextInt(1_000_000);
                Integer old = reference.put(key, value);
                assertEquals(old == null ? MISSING : old, map.put(key, value), "put at step " + step);
            } else {
                Integer old = reference.remove(key);
                assertEquals(old == null ? MISSING : old, map.remove(key), "remove at step " + step);
            }
            assertEquals(reference.size(), map.size());
            if (step % 1000 == 0) {
                for (long k = 0; k < maxEntries * 2; k++) {
                    Integer expected = reference.get(k);
                    assertEquals(expected == null ? MISSING : expected, map.get(k), "get " + k + " at step " + step);
                }
            }
        }
    }
}
