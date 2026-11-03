package com.berker.db.storage;

/**
 * How a {@link PageGuard} holds its page's latch.
 *
 * <p>A latch protects a page's <em>bytes</em> while one physical change or read is in
 * progress, and is held only that long (microseconds). It is not a transaction lock: keeping
 * two transactions from overwriting each other's rows until commit is the job of concurrency
 * control, a layer above the BufferPool.
 */
public enum LatchMode {
    /** Shared: any number of threads may read the page at the same time; no one may change it. */
    READ,
    /** Exclusive: one thread may read and change the page; no one else may even read it. */
    WRITE
}
