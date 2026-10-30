package com.berker.db.storage;

import java.util.Arrays;

/**
 * A fixed-capacity map from {@code long} keys to {@code int} values that never allocates after
 * construction. The BufferPool's page table: page key to frame number.
 *
 * <p><b>Why not {@code HashMap<Long, Integer>}.</b> It boxes: looking up a {@code long}
 * creates a {@code Long}, storing an {@code int} creates an {@code Integer}, and every entry
 * is a separate node object. That's allocation on every page access.
 *
 * <p><b>How it works.</b> Open addressing with linear probing: keys and values sit in two
 * parallel arrays. A key's "home" slot comes from its hash; if that slot is taken, the key
 * goes in the next free slot after it. A lookup starts at the home slot and walks forward
 * until it finds the key or an empty slot.
 *
 * <p><b>Never resizes.</b> The BufferPool has a fixed number of frames, so the map never holds
 * more than that many entries. The arrays get at least twice that many slots, so they are at
 * most half full and probe chains stay short.
 *
 * <p><b>Deletion by backward shift.</b> Simply emptying a slot would break lookups: a key
 * stored further along the chain would no longer be found, because the walk stops at the
 * new hole. The common fix is a "tombstone" marker, but tombstones pile up as pages churn
 * through the pool and make every lookup slower over time. Instead, {@link #remove} moves
 * later entries of the chain back into the hole, so the table looks exactly as if the
 * removed key had never been inserted.
 *
 * <p>Not thread-safe; the BufferPool only uses it under its latch.
 */
final class LongIntHashMap {

    /** Returned by {@link #get} and {@link #remove} when the key is absent. */
    static final int MISSING = -1;

    /** Marks an empty slot. Real keys are never negative (see {@link PageKeys}). */
    private static final long EMPTY = -1L;

    /** 2^64 / golden ratio. Multiplying by it spreads nearby keys over the whole table. */
    private static final long GOLDEN = 0x9E37_79B9_7F4A_7C15L;

    private final long[] keys;
    private final int[] values;
    private final int mask;
    private final int shift;
    private final int maxEntries;
    private int size;

    LongIntHashMap(int maxEntries) {
        if (maxEntries < 1 || maxEntries > (1 << 29)) {
            throw new IllegalArgumentException("maxEntries out of range: " + maxEntries);
        }
        int capacity = Math.max(2, Integer.highestOneBit(2 * maxEntries - 1) << 1);
        this.keys = new long[capacity];
        this.values = new int[capacity];
        this.mask = capacity - 1;
        this.shift = 64 - Integer.numberOfTrailingZeros(capacity);
        this.maxEntries = maxEntries;
        Arrays.fill(keys, EMPTY);
    }

    /** The value for {@code key}, or {@link #MISSING}. */
    int get(long key) {
        for (int slot = home(key); ; slot = (slot + 1) & mask) {
            long k = keys[slot];
            if (k == key) {
                return values[slot];
            }
            if (k == EMPTY) {
                return MISSING;
            }
        }
    }

    /**
     * Stores {@code value} under {@code key}, replacing any previous value.
     *
     * @return the previous value, or {@link #MISSING}
     * @throws IllegalStateException if the key is new and the map already holds maxEntries keys
     */
    int put(long key, int value) {
        if (key < 0) {
            throw new IllegalArgumentException("keys must not be negative: " + key);
        }
        if (value < 0) {
            throw new IllegalArgumentException("values must not be negative: " + value);
        }
        int slot = home(key);
        while (true) {
            long k = keys[slot];
            if (k == key) {
                int previous = values[slot];
                values[slot] = value;
                return previous;
            }
            if (k == EMPTY) {
                break;
            }
            slot = (slot + 1) & mask;
        }
        if (size == maxEntries) {
            throw new IllegalStateException("map is full (" + maxEntries + " entries)");
        }
        keys[slot] = key;
        values[slot] = value;
        size++;
        return MISSING;
    }

    /**
     * Removes {@code key}.
     *
     * @return its value, or {@link #MISSING} if it wasn't there
     */
    int remove(long key) {
        int hole = home(key);
        while (true) {
            long k = keys[hole];
            if (k == key) {
                break;
            }
            if (k == EMPTY) {
                return MISSING;
            }
            hole = (hole + 1) & mask;
        }
        int removed = values[hole];

        // Walk the rest of the chain. An entry may move back into the hole only if that
        // doesn't put it before its home slot: its home must not lie strictly between the
        // hole and its current slot (all positions measured cyclically).
        int slot = hole;
        while (true) {
            slot = (slot + 1) & mask;
            long k = keys[slot];
            if (k == EMPTY) {
                break;
            }
            int home = home(k);
            int homeToSlot = (slot - home) & mask;
            int holeToSlot = (slot - hole) & mask;
            if (homeToSlot >= holeToSlot) {
                keys[hole] = k;
                values[hole] = values[slot];
                hole = slot;
            }
        }
        keys[hole] = EMPTY;
        size--;
        return removed;
    }

    int size() {
        return size;
    }

    /**
     * Fibonacci hashing: multiply, keep the top bits. Page keys are very regular (pageNo
     * counts up, fileId sits in the high half), and the multiplication mixes every input
     * bit into the bits we keep. A plain {@code key & mask} would ignore the fileId entirely.
     */
    private int home(long key) {
        return (int) ((key * GOLDEN) >>> shift);
    }
}
