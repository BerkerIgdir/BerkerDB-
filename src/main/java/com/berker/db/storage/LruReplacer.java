package com.berker.db.storage;

import java.util.Arrays;

/**
 * Chooses which buffer frame to reuse: the one unpinned for the longest time (least recently
 * used, LRU). Only unpinned frames are in it; a pinned frame can never be chosen.
 *
 * <p>It's a doubly linked list of frame numbers, but the links live in two {@code int}
 * arrays indexed by frame instead of node objects. Adding, removing and picking a victim are
 * all O(1) and allocate nothing. {@code java.util.LinkedHashMap} could do LRU too, but it
 * boxes keys and allocates a node per entry.
 *
 * <p>The head is the least recently used frame (next victim), the tail the most recent.
 *
 * <p>Known weakness, from the book: one large sequential scan pushes every other page out
 * ("sequential flooding"). Scan-resistant policies (clock, LRU-K, ARC) can replace this
 * class later without touching the BufferPool's other parts.
 *
 * <p>Not thread-safe; the BufferPool only uses it under its latch.
 */
final class LruReplacer {

    private static final int NONE = -1;

    private final int[] prev;
    private final int[] next;
    private final boolean[] inList;
    private int head = NONE;
    private int tail = NONE;
    private int size;

    LruReplacer(int frameCount) {
        prev = new int[frameCount];
        next = new int[frameCount];
        inList = new boolean[frameCount];
        Arrays.fill(prev, NONE);
        Arrays.fill(next, NONE);
    }

    /** The frame became unpinned: it is now the most recently used. Moves it if already listed. */
    void add(int frame) {
        if (inList[frame]) {
            unlink(frame);
        }
        prev[frame] = tail;
        next[frame] = NONE;
        if (tail == NONE) {
            head = frame;
        } else {
            next[tail] = frame;
        }
        tail = frame;
        inList[frame] = true;
        size++;
    }

    /** The frame got pinned: it must not be chosen. Does nothing if it isn't listed. */
    void remove(int frame) {
        if (inList[frame]) {
            unlink(frame);
        }
    }

    /** Removes and returns the least recently used frame, or -1 if there is none. */
    int pollVictim() {
        int victim = head;
        if (victim != NONE) {
            unlink(victim);
        }
        return victim;
    }

    boolean contains(int frame) {
        return inList[frame];
    }

    int size() {
        return size;
    }

    private void unlink(int frame) {
        int p = prev[frame];
        int n = next[frame];
        if (p == NONE) {
            head = n;
        } else {
            next[p] = n;
        }
        if (n == NONE) {
            tail = p;
        } else {
            prev[n] = p;
        }
        prev[frame] = NONE;
        next[frame] = NONE;
        inList[frame] = false;
        size--;
    }
}
