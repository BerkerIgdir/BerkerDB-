package com.berker.db.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Reads and writes whole pages of table files. One instance serves every table file;
 * a page is addressed by {@code (fileId, pageNo)} and lives at byte offset
 * {@code pageNo * PAGE_SIZE} in its file.
 *
 * <p>The DiskManager knows nothing about page contents and caches nothing - that is the
 * BufferPool's job. It only moves 8 KB blocks between files and off-heap memory.
 *
 * <p><b>Hot path.</b> {@link #readPage(int, int, ByteBuffer)}, {@link #writePage(int, int, ByteBuffer)}
 * and {@link #allocatePage(int)} allocate nothing. {@code FileChannel} only accepts
 * {@code ByteBuffer}s, and turning a {@code MemorySegment} into one ({@code asByteBuffer()})
 * creates a new wrapper object each time. So the hot-path methods take a direct
 * {@code ByteBuffer} that the caller created once (the BufferPool makes one per frame at
 * startup) and reuses forever. The {@code MemorySegment} overloads are conveniences for
 * tests and cold paths, and they do allocate that wrapper.
 *
 * <p><b>Durability.</b> Writes go to the OS page cache and are not durable until
 * {@link #sync(int)} or {@link #syncAll()}. fsync costs milliseconds, so the caller
 * (later: the WAL / checkpoint code) decides when it is needed.
 *
 * <p><b>Threads.</b> Every method may be called from any thread.
 * <ul>
 *   <li>{@code readPage} / {@code writePage} take no lock. Positional FileChannel I/O passes
 *       the offset inside the call, so there is no shared file cursor to protect.</li>
 *   <li>{@code allocatePage}, {@code openFile}, {@code closeFile} and {@code close} change
 *       shared bookkeeping and run under one lock.</li>
 *   <li>The caller must not run two I/Os on the <em>same page</em> at once, nor share a
 *       buffer between threads (a torn page would result). The BufferPool guarantees this
 *       with its frame latches; the DiskManager can't know which requests conflict.</li>
 *   <li>The caller must not close a file while I/O on it is in flight. Such I/O fails with
 *       an exception; it never lands in another file.</li>
 *   <li>Interrupts: Java closes a FileChannel for <em>every</em> thread when a thread is
 *       interrupted during I/O on it. The interrupted thread's call fails (cause
 *       {@link ClosedByInterruptException}, interrupt flag still set). The DiskManager then
 *       reopens the file, so other threads retry transparently and the table stays usable.</li>
 * </ul>
 *
 * <p><b>Processes.</b> All of the above is about threads inside <em>one</em> process. Two
 * processes writing one table file would corrupt it, because each one's page counts and
 * cached pages go stale behind the other's back. So {@link #openFile} takes an exclusive OS
 * file lock and refuses a file that is already locked (H2's MVStore does the same). One catch:
 * the OS lock belongs to the channel, so when an interrupt closes the channel the lock is
 * released too. {@code reopen} takes it again, but in the short gap between the close and the
 * re-lock another process could get it first. Then that fileId fails from then on instead of
 * being shared.
 */
public final class DiskManager implements AutoCloseable {

    public static final int PAGE_SIZE = 8192;

    /**
     * Upper bound for fileIds. The per-file arrays are allocated at full size up front
     * (~1 MB in total), so they never have to grow. Growing a shared array means
     * publishing a new array to other threads, which is exactly the kind of race we
     * would rather not have at all.
     */
    public static final int MAX_FILE_ID = 0xFFFF;

    private static final int MAX_FILES = MAX_FILE_ID + 1;

    // Read on the hot path without a lock, so they need cross-thread visibility. The atomic
    // arrays give every element volatile semantics: a write in one thread is seen by later
    // reads in all threads. get() on them allocates nothing and compiles to a plain load on x86.
    private final AtomicReferenceArray<FileChannel> channels = new AtomicReferenceArray<>(MAX_FILES);
    private final AtomicIntegerArray pageCounts = new AtomicIntegerArray(MAX_FILES);

    // Only touched while holding `lock`, so plain arrays are enough.
    private final Path[] paths = new Path[MAX_FILES];
    // When an interrupt killed a channel and we reopened the file: the dead channel it replaced.
    // Lets a thread that still holds the dead channel tell "reopened, retry" apart from
    // "closed on purpose, maybe a different file under the same id now, don't retry".
    private final FileChannel[] reopenedFrom = new FileChannel[MAX_FILES];

    /**
     * {@code synchronized} rather than {@code ReentrantLock}: a contended ReentrantLock
     * allocates a queue node for every waiting thread, while a contended monitor allocates
     * nothing on the Java heap.
     */
    private final Object lock = new Object();

    // Shared (not confined) so it may be used from any thread.
    private final Arena arena = Arena.ofShared();
    // Source for allocatePage(). Only used while holding `lock`, because its position and
    // limit change during a write.
    private final ByteBuffer zeroPage = arena.allocate(PAGE_SIZE).asByteBuffer();

    private volatile boolean closed;

    // ---------------------------------------------------------------- files (cold path)

    /**
     * Opens (creating if missing) the file for {@code fileId}. The fileId is chosen by the
     * caller - later the catalog - because it must mean the same table after a restart.
     *
     * <p>Takes an exclusive OS lock on the file, held until it is closed (see "Processes" in
     * the class comment).
     *
     * @throws IllegalStateException if the fileId is already open, the file is already open in
     *         this process or locked by another one, or the file length is not a whole number
     *         of pages (usually a crash while the file was being extended)
     */
    public void openFile(int fileId, Path path) {
        if (fileId < 0 || fileId > MAX_FILE_ID) {
            throw new IllegalArgumentException("fileId out of range [0, " + MAX_FILE_ID + "]: " + fileId);
        }
        synchronized (lock) {
            ensureOpen();
            if (channels.get(fileId) != null) {
                throw new IllegalStateException("fileId " + fileId + " is already open");
            }

            FileChannel channel = null;
            try {
                channel = FileChannel.open(path,
                        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                // Lock before looking at the size: until we own the file, another process may
                // still be growing it.
                lockExclusively(channel, path);
                long size = channel.size();
                if (size % PAGE_SIZE != 0) {
                    throw new IllegalStateException("file " + path + " has length " + size
                            + ", which is not a multiple of the page size " + PAGE_SIZE);
                }
                long pages = size / PAGE_SIZE;
                if (pages > Integer.MAX_VALUE) {
                    throw new IllegalStateException("file " + path + " has more pages than an int page number can address");
                }
                paths[fileId] = path;
                reopenedFrom[fileId] = null;
                pageCounts.set(fileId, (int) pages);
                // Publish the channel last: a thread that sees it also sees the page count.
                channels.set(fileId, channel);
            } catch (IOException e) {
                closeQuietly(channel);
                throw new UncheckedIOException("cannot open " + path, e);
            } catch (RuntimeException e) {
                closeQuietly(channel);
                throw e;
            }

        }
    }

    /**
     * Closes the file for {@code fileId}. This does <em>not</em> sync: call {@link #sync(int)}
     * first if the written pages must survive a crash. No I/O on this file may be in flight.
     */
    public void closeFile(int fileId) {
        synchronized (lock) {
            FileChannel channel = channel(fileId);
            // Unregister before closing, so a thread whose I/O fails because of this close
            // can see it was deliberate and must not retry.
            unregister(fileId);
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public int pageCount(int fileId) {
        channel(fileId);
        return pageCounts.get(fileId);
    }

    // ---------------------------------------------------------------- pages (hot path)

    /**
     * Reads page {@code pageNo} of {@code fileId} into {@code dst}.
     *
     * @param dst a direct buffer of capacity exactly {@link #PAGE_SIZE}. Its position and
     *            limit are overwritten; treat them as scratch state.
     */
    public void readPage(int fileId, int pageNo, ByteBuffer dst) {
        FileChannel channel = channel(fileId);
        // checkIndex is a JIT intrinsic: on the success path it compiles to one compare,
        // and it builds its exception message only when it actually throws.
        Objects.checkIndex(pageNo, pageCounts.get(fileId));
        checkPageBuffer(dst);
        long offset = offsetOf(pageNo);
        while (true) {
            dst.clear();
            try {
                readFully(channel, dst, offset);
                return;
            } catch (ClosedChannelException e) {
                channel = channelAfterClose(fileId, channel, e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Overwrites existing page {@code pageNo} of {@code fileId} with {@code src}. New pages
     * are created with {@link #allocatePage(int)}, never by writing past the end.
     *
     * @param src a direct buffer of capacity exactly {@link #PAGE_SIZE}; all of it is written
     *            regardless of its position and limit.
     */
    public void writePage(int fileId, int pageNo, ByteBuffer src) {
        FileChannel channel = channel(fileId);
        Objects.checkIndex(pageNo, pageCounts.get(fileId));
        checkPageBuffer(src);
        long offset = offsetOf(pageNo);
        while (true) {
            // Retrying rewrites the whole page, so a write cut short by a closed channel is
            // simply repeated in full.
            src.clear();
            try {
                writeFully(channel, src, offset);
                return;
            } catch (ClosedChannelException e) {
                channel = channelAfterClose(fileId, channel, e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Appends a zero-filled page to the file and returns its page number.
     *
     * <p>The zeros are written right away, so the file length always equals
     * {@code pageCount * PAGE_SIZE}. If we crash halfway through, the length is not a
     * multiple of the page size and {@link #openFile} refuses the file instead of guessing.
     * The count is bumped only after the write succeeded, so a concurrent reader that sees
     * the new count can read the page.
     *
     * <p>Runs under the lock: two threads must not both claim the same page number.
     * Allocation is rare next to reads and writes, so one lock is fine for now.
     */
    public int allocatePage(int fileId) {
        synchronized (lock) {
            FileChannel channel = channel(fileId);
            int pageNo = pageCounts.get(fileId);
            if (pageNo == Integer.MAX_VALUE) {
                throw new IllegalStateException("fileId " + fileId + " is full");
            }
            long offset = offsetOf(pageNo);
            while (true) {
                zeroPage.clear();
                try {
                    writeFully(channel, zeroPage, offset);
                    break;
                } catch (ClosedChannelException e) {
                    // Reopening also cuts off the partial page this write may have left.
                    channel = channelAfterClose(fileId, channel, e);
                } catch (IOException e) {
                    // E.g. disk full. Don't leave a partial page at the end of the file.
                    truncateQuietly(channel, offset);
                    throw new UncheckedIOException(e);
                }
            }
            pageCounts.set(fileId, pageNo + 1);
            return pageNo;
        }
    }

    // ---------------------------------------------------------------- MemorySegment conveniences

    /** Like {@link #readPage(int, int, ByteBuffer)}, but allocates a ByteBuffer view. Not for hot paths. */
    public void readPage(int fileId, int pageNo, MemorySegment dst) {
        readPage(fileId, pageNo, pageBufferOf(dst));
    }

    /** Like {@link #writePage(int, int, ByteBuffer)}, but allocates a ByteBuffer view. Not for hot paths. */
    public void writePage(int fileId, int pageNo, MemorySegment src) {
        writePage(fileId, pageNo, pageBufferOf(src));
    }

    // ---------------------------------------------------------------- durability

    /**
     * Forces the file's written pages to stable storage.
     *
     * <p>Why {@code force(true)}: the spec of {@code force(false)} only promises the file's
     * <em>content</em>, and the file length is metadata. After {@link #allocatePage} the new
     * length must survive a crash too. (In practice Linux's fdatasync and Windows'
     * FlushFileBuffers do flush the length, but that is not something the API guarantees.)
     *
     * <p>No lock: fsync takes milliseconds and must not block allocations.
     */
    public void sync(int fileId) {
        FileChannel channel = channel(fileId);
        while (true) {
            try {
                channel.force(true);
                return;
            } catch (ClosedChannelException e) {
                // fsync works on the file, not the handle, so syncing the reopened
                // channel also flushes what was written through the dead one.
                channel = channelAfterClose(fileId, channel, e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public void syncAll() {
        ensureOpen();
        for (int fileId = 0; fileId < MAX_FILES; fileId++) {
            if (channels.get(fileId) != null) {
                sync(fileId);
            }
        }
    }

    /**
     * Closes every open file (without syncing) and frees the off-heap memory. Idempotent.
     * No I/O may be in flight.
     */
    @Override
    public void close() {
        IOException failure = null;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            for (int fileId = 0; fileId < MAX_FILES; fileId++) {
                FileChannel channel = channels.get(fileId);
                if (channel == null) {
                    continue;
                }
                unregister(fileId);
                try {
                    channel.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            arena.close();
        }
        if (failure != null) {
            throw new UncheckedIOException(failure);
        }
    }

    // ---------------------------------------------------------------- closed channels

    /**
     * Called when I/O on {@code dead} failed because the channel was closed. Works out why,
     * and either returns a live channel to retry with or throws.
     *
     * <ul>
     *   <li>{@code dead} is still registered: nobody closed it on purpose (closeFile and close
     *       unregister first), so an interrupt did. Reopen the file.</li>
     *   <li>Another thread already reopened it after that interrupt: use its channel.</li>
     *   <li>Otherwise it was closed on purpose: fail. Retrying could write into a different
     *       file that was opened under the same fileId in the meantime.</li>
     * </ul>
     *
     * The interrupted thread itself always fails, even after reopening: its interrupt means
     * "stop", and the caller should see that.
     *
     * <p>Only runs after a failure, so it may take the lock and allocate.
     */
    private FileChannel channelAfterClose(int fileId, FileChannel dead, ClosedChannelException cause) {
        FileChannel live;
        synchronized (lock) {
            FileChannel current = channels.get(fileId);
            if (current == dead) {
                live = reopen(fileId);
            } else if (current != null && reopenedFrom[fileId] == dead) {
                live = current;
            } else {
                live = null;
            }
        }
        if (live == null || cause instanceof ClosedByInterruptException) {
            throw new UncheckedIOException(cause);
        }
        return live;
    }

    /** Caller holds {@code lock}. Replaces the dead channel of {@code fileId} with a new one. */
    private FileChannel reopen(int fileId) {
        // FileChannel operations close the channel at once if the calling thread's interrupt
        // flag is set - that would kill the new channel too. So clear the flag while
        // reopening and put it back afterwards.
        boolean interrupted = Thread.interrupted();
        FileChannel fresh = null;
        try {
            fresh = FileChannel.open(paths[fileId], StandardOpenOption.READ, StandardOpenOption.WRITE);
            // The OS lock died with the old channel, so take it again - before the truncate
            // below, which must never touch a file another process now owns. If another process
            // got in during the gap, this throws and the fileId stays dead: every later I/O
            // tries to reopen and fails the same way, rather than writing into a shared file.
            lockExclusively(fresh, paths[fileId]);
            // A failed allocatePage may have left a partial page at the end; cut it off so
            // the file length is a whole number of pages again.
            fresh.truncate(offsetOf(pageCounts.get(fileId)));
            reopenedFrom[fileId] = channels.get(fileId);
            channels.set(fileId, fresh);
            return fresh;
        } catch (IOException e) {
            closeQuietly(fresh);
            throw new UncheckedIOException("cannot reopen fileId " + fileId + " (" + paths[fileId] + ")", e);
        } catch (RuntimeException e) {
            closeQuietly(fresh);
            throw e;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    /**
     * The cast to long matters: {@code pageNo * PAGE_SIZE} in int arithmetic overflows at
     * page 262,144 (the 2 GB mark) and would silently read or write the wrong place.
     */
    private static long offsetOf(int pageNo) {
        return (long) pageNo * PAGE_SIZE;
    }

    /**
     * FileChannel may transfer fewer bytes than asked ("short read"), so loop until the buffer
     * is full. Positional reads don't move the channel's own position, but they do advance
     * the buffer's, which is why the file offset is {@code offset + buf.position()}.
     */
    private static void readFully(FileChannel channel, ByteBuffer buf, long offset) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf, offset + buf.position());
            if (n < 0) {
                throw new EOFException("end of file at offset " + (offset + buf.position()));
            }
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf, long offset) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf, offset + buf.position());
        }
    }

    /**
     * Takes an exclusive OS lock on the whole file, or throws if someone else holds one.
     *
     * <ul>
     *   <li>{@code tryLock}, not {@code lock}: {@code lock} would wait until the other owner
     *       lets go. A file that is in use is a configuration error, so we report it right away.</li>
     *   <li>The JVM tracks its own locks per file. A second lock on the same file from this
     *       process (same file under two fileIds, or a second DiskManager) throws
     *       {@link OverlappingFileLockException} instead of returning null.</li>
     *   <li>The returned {@code FileLock} is not kept: closing the channel releases it, and
     *       we never release it any other way.</li>
     *   <li>How strong the lock is depends on the OS. Windows locks are mandatory: other
     *       processes can't even read the locked bytes. Linux/macOS locks are advisory: they
     *       only stop programs that also ask for a lock, so a plain {@code cp} still works.</li>
     * </ul>
     */
    private static void lockExclusively(FileChannel channel, Path path) throws IOException {
        FileLock fileLock;
        try {
            fileLock = channel.tryLock(0, Long.MAX_VALUE, false);
        } catch (OverlappingFileLockException e) {
            throw new IllegalStateException("file " + path + " is already open in this process", e);
        }
        if (fileLock == null) {
            throw new IllegalStateException("file " + path + " is locked by another process");
        }
    }

    private FileChannel channel(int fileId) {
        FileChannel channel = fileId >= 0 && fileId < MAX_FILES ? channels.get(fileId) : null;
        if (channel == null) {
            throw new IllegalArgumentException("fileId " + fileId + " is not open");
        }
        return channel;
    }

    /** Caller holds {@code lock}. */
    private void unregister(int fileId) {
        channels.set(fileId, null);
        pageCounts.set(fileId, 0);
        paths[fileId] = null;
        reopenedFrom[fileId] = null;
    }

    /**
     * Direct only: with a heap buffer, FileChannel silently copies through a temporary
     * direct buffer, so the data would bounce through the Java heap.
     */
    private static void checkPageBuffer(ByteBuffer buf) {
        if (!buf.isDirect() || buf.capacity() != PAGE_SIZE) {
            throw new IllegalArgumentException("page buffer must be direct with capacity " + PAGE_SIZE);
        }
    }

    private static ByteBuffer pageBufferOf(MemorySegment segment) {
        if (!segment.isNative() || segment.byteSize() != PAGE_SIZE) {
            throw new IllegalArgumentException("page segment must be native with size " + PAGE_SIZE);
        }
        return segment.asByteBuffer();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("DiskManager is closed");
        }
    }

    private static void truncateQuietly(FileChannel channel, long size) {
        try {
            channel.truncate(size);
        } catch (IOException ignored) {
            // already failing; if this fails too, openFile will spot the bad length later
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // already failing; the original exception is the one worth reporting
        }
    }
}
