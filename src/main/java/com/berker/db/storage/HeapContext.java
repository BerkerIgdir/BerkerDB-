package com.berker.db.storage;

/**
 * The reusable objects one worker needs to call {@link TableHeap}. A TableHeap is shared between
 * threads, but a {@link PageGuard} and a {@link SlottedPage} each belong to one thread, so they
 * can't be fields of the heap, and creating them per call would allocate per row. Create one
 * context per worker and pass it in. A ThreadLocal would hide this, but it is global state in
 * disguise, and the wiring here is explicit everywhere else.
 */
public final class HeapContext {

    final PageGuard guard = new PageGuard();
    final SlottedPage page = new SlottedPage();
}
