# BerkerDB

A relational database written in Java, built to learn database internals.
The owner reads *Database System Concepts* (Silberschatz) chapter by chapter
and implements each concept after reading it.

## How to work on this project

- This is a learning project. When implementing something, explain *why* it is
  built that way, especially where the low-GC style differs from ordinary Java.
- Point out decisions the book leaves open (byte layouts, error cases, etc.).
- Build bottom-up, one layer at a time, with tests for each layer before moving up.
- Prefer small, reviewable changes.

## Tech stack

- Java 25 (LTS), standard HotSpot JIT. No GraalVM native image.
- Off-heap memory via the Foreign Function & Memory API (`MemorySegment`, `Arena`).
  Do NOT use `sun.misc.Unsafe`.

## Low-GC rules

- Hot paths (per-page and per-tuple code: page access, tuple reads, scans,
  index lookups, joins, log writes) must allocate nothing.
- Use flyweights that `wrap()` memory instead of allocating objects per row.
- No boxing, no streams or capturing lambdas, no string concatenation, no
  standard-collection iterators in hot paths. Use primitive arrays.
- Compare strings as bytes in memory; never decode to `String` in hot paths.
- Cold paths (catalog, schema, parser, planner, startup) may use normal idiomatic Java.
- Keep FFM bounds checks on.

## Storage design

- Fixed page size: 8 KB. One file per table; page N is at offset N * PAGE_SIZE.
- One shared `DiskManager` serves all table files. Each file has a small integer
  `fileId`; a page is identified by `(fileId, pageNo)`. The BufferPool packs this
  into a `long` key (fileId high 32 bits, pageNo low 32 bits).
- fileIds are assigned by the catalog and must stay stable across restarts (the WAL
  will reference them). A dropped table's fileId is never reused.
- Slotted pages:
  - Header: slot count, free-space start, free-space end, flags, reserved space for an LSN.
  - Slot array grows forward from the header; each slot = offset (2 bytes) + length (2 bytes).
  - Tuples grow backward from the end of the page; free space is one contiguous gap.
  - Delete marks the slot empty; compaction is lazy and only changes slot offsets.
- Records never span pages. Enforce a max tuple size (overflow/TOAST-like mechanism later).
- Record ID = (page number, slot number) packed into one `long`. It has no fileId:
  the table that owns the record already knows its file. Keep RID and page-key
  packing helpers separate so the two `long` layouts are never mixed up.
- Tuple format: null bitmap, fixed-length columns at precomputed offsets, then
  variable-length columns located via an offset table inside the tuple.

## Components (build order)

1. `DiskManager` - read/write whole pages by `(fileId, pageNo)` using `FileChannel`,
   directly into/out of off-heap memory (no byte arrays). Per-file state lives in
   arrays indexed by fileId. Hot-path I/O takes direct `ByteBuffer`s that the
   BufferPool creates once per frame at startup. Writes don't fsync; `sync()` does (`force(true)`, so a grown file length is durable too).
   Thread-safe: read/write are lock-free (positional I/O); allocate/open/close share one
   `synchronized` lock. Callers must not run two I/Os on the same page at once or close a
   file with I/O in flight. A channel closed by a thread interrupt is reopened transparently.
   Each open file holds an exclusive OS lock (`tryLock`, re-taken on reopen), so a second
   process - or a second open of the same file in this process - is refused.
2. `BufferPool` - one large 4096-aligned `MemorySegment` split into frames. One `Frame`
   object per frame (page key, pin count, dirty flag, slice + ByteBuffer view), all created
   at startup. Page table = `LongIntHashMap` (fixed capacity, backward-shift deletion);
   eviction = `LruReplacer` (linked list in int arrays); free frames used first.
   Callers get pages through a reusable `PageGuard`: `fetchPage(fileId, pageNo, LatchMode, guard)`
   pins and latches (READ = shared, read-only segment; WRITE = exclusive, `markDirty` allowed);
   `newPage` always gives WRITE; `close()` releases the latch, then unpins. Page access
   allocates nothing. All frames pinned -> `BufferPoolFullException`.
   Two latch kinds: the global pool latch (`synchronized`) guards bookkeeping and is still held
   during miss I/O; a per-frame `StampedLock` page latch guards page bytes. Rule: never wait for
   a page latch while holding the pool latch (order: flushLock -> page latch -> pool latch).
   Pin count is an `AtomicInteger`: pins happen under the pool latch; unpins are lock-free and
   only the last one (count -> 0) takes the pool latch, re-checking the count and that the frame
   isn't empty before adding it to the replacer. The dirty flag is volatile, set before unpinning.
   StampedLock isn't reentrant: re-fetching a page the thread holds with WRITE throws; READ then
   WRITE on the same page by one thread deadlocks (undetected). Flushing takes the READ latch and
   must be called with no guards held. Page latches are short (one physical change); keeping
   transactions apart (locks/MVCC) is a later layer. Releasing the pool latch during I/O comes later.
   Components are created at startup and passed via constructors, never singletons.
3. `SlottedPage` - flyweight over a page; insert, read, delete, free space, compact.
4. `Schema` / `TupleLayout` - column definitions and precomputed offsets (cold path).
5. `TupleView` - flyweight over one tuple's (offset, length) range.
6. `TableHeap` - insert into a page with space, full table scan, reusing flyweights.

## Testing

- Unit tests per class for correctness.
- Hot-path tests also run under Epsilon GC (`-XX:+UseEpsilonGC`) to prove zero allocation.
- JMH for any performance measurement; no hand-rolled timing loops.
