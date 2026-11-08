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
- Write all explanations in ASD-STE100 Simplified Technical English: approved words with one
  meaning each, active voice, short sentences (procedural up to 20 words, descriptive up to 25),
  one instruction per sentence, and one topic per paragraph. Technical names (class names,
  database terms) are allowed. Terms must stay the same throughout.

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
- Slotted pages (book design, Silberschatz 13.2.2). All values little-endian.
  - Header, 24 bytes: pageLSN (8, reserved for the WAL), slotCount (u16), freeEnd (u16),
    flags (u16), 10 reserved bytes. freeStart isn't stored: it's `24 + 4 * slotCount`.
  - Slot array grows forward from the header; each slot = offset (u16) + length (u16).
    A deleted slot has length 0xFFFF (the book's size -1).
  - Tuples grow backward from the end of the page, each starting at a multiple of 8
    (occupying its length rounded up to 8); free space is always one contiguous gap.
  - Delete compacts immediately: tuples stored below the deleted one slide up, and their
    slots get the new offsets (compare offsets, not slot numbers). Freed bytes are zeroed.
    Slot numbers never change, so RIDs stay valid; never keep a tuple offset across a latch release.
  - Insert reuses the first deleted slot (revisit once indexes hold RIDs). It returns -1 when
    the tuple doesn't fit; lengths outside 1..8160 throw.
  - Update keeps the slot: same size (rounded to 8) is overwritten in place; another size removes
    the old bytes as delete does and puts the new ones at freeEnd. Returns false, page unchanged,
    if the gap plus the old size isn't enough.
- Records never span pages. Max tuple size 8160 (an empty page; overflow/TOAST-like mechanism later).
- Record ID = (page number, slot number) packed into one `long`. It has no fileId:
  the table that owns the record already knows its file. Keep RID and page-key
  packing helpers separate so the two `long` layouts are never mixed up.
- Tuple format (book order, 13.2.1): an (offset, length) pair per variable-length column
  (two u16s; offset relative to the tuple's start), padding up to the largest fixed column's
  size, fixed-length columns at precomputed offsets (stored largest first, ties in declared
  order, so they're aligned), the null bitmap (one bit per column, nullable or not), then the
  variable-length bytes. A NULL fixed column keeps its bytes. No per-tuple header yet (MVCC
  will add one).
- Types: BOOLEAN, INT, BIGINT, DOUBLE, VARCHAR(n), with n in bytes (UTF-8, never decoded).
  Only the fixed part must fit on a page: VARCHAR limits may add up to more, and a row that
  really is too long is refused at insert.

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
3. `SlottedPage` - flyweight over a page (`wrap`, `init`); insert, read (`tupleOffset`/`tupleLength`),
   update in the same slot, delete with immediate compaction, `freeSpace`. No latching: the
   caller holds a `PageGuard`.
4. `Schema` / `TupleLayout` - column definitions (`Column`, `ColumnType`) in declared order, and
   offsets computed once into primitive arrays, addressed by declared column index. Built on the
   cold path; `TupleLayout`'s accessors are read per column and allocate nothing.
5. `TupleView` - flyweight over one tuple's (offset, length) range: typed getters by declared
   column index, `isNull`, and `compareVarchar` (unsigned bytes, no decoding). `TupleWriter` is
   its opposite: builds a row in a reusable buffer; columns start NULL, set in any order.
6. `TableHeap` - shared by threads; insert, and `read`, `update` and `delete` by RID (`Rids`).
   An update that fits on the row's page keeps the RID; otherwise the row moves (insert the new
   version, then delete the old one, no latch held between) and `update` returns a new RID,
   which callers such as indexes must take over. No forwarding pointers. Per-worker
   objects live in a `HeapContext` passed to each call. Free-space map as in the book: one byte
   per page (free bytes / 32, rounded down), searched from the last page used; not stored,
   rebuilt by reading every page on open. Its lock comes after the page latch.
   Rows are copied out, so no latch is held while the caller works on a row: `read` copies the
   row, and `TableScan` copies each page into its own 8 KB buffer (one fetch per page). No
   transactions yet: a concurrent scan can see a moving row twice for a moment.

## Testing

- Unit tests per class for correctness.
- Hot-path tests also run under Epsilon GC (`-XX:+UseEpsilonGC`) to prove zero allocation,
  with `-Xbatch` (synchronous JIT) and a warmup that repeats the measured call, so the
  steady state is measured, not the JIT transitions.
- JMH for any performance measurement; no hand-rolled timing loops.
