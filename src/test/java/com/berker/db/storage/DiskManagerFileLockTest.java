package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The OS file lock that stops two openers - two processes, or two opens in one - sharing a file. */
class DiskManagerFileLockTest {

    // Exit codes of the child process in Child.main.
    private static final int OPENED = 0;
    private static final int LOCKED = 3;

    @TempDir
    Path dir;

    private DiskManager disk;

    @BeforeEach
    void setUp() {
        disk = new DiskManager();
    }

    @AfterEach
    void tearDown() {
        disk.close();
    }

    @Test
    void sameFileUnderTwoFileIdsIsRefused() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> disk.openFile(1, file));
        assertTrue(e.getMessage().contains("already open in this process"), e.getMessage());
        // The failed open must not leave fileId 1 half-registered.
        assertThrows(IllegalArgumentException.class, () -> disk.pageCount(1));
    }

    @Test
    void secondDiskManagerOnTheSameFileIsRefused() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        try (DiskManager other = new DiskManager()) {
            assertThrows(IllegalStateException.class, () -> other.openFile(0, file));
        }
    }

    @Test
    void closingTheFileReleasesTheLock() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        disk.closeFile(0);
        try (DiskManager other = new DiskManager()) {
            other.openFile(0, file);
        }
    }

    @Test
    void lockIsTakenAgainAfterAnInterruptReopensTheFile() {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        disk.allocatePage(0);
        try (Arena arena = Arena.ofConfined(); DiskManager other = new DiskManager()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            Thread.currentThread().interrupt();
            assertThrows(UncheckedIOException.class, () -> disk.readPage(0, 0, page));
            assertTrue(Thread.interrupted());

            // The interrupt killed the channel and its lock; reopen must have locked again.
            assertThrows(IllegalStateException.class, () -> other.openFile(0, file));
            disk.readPage(0, 0, page);
        }
    }

    @Test
    void anotherProcessCannotOpenALockedFile() throws Exception {
        Path file = dir.resolve("t0");
        disk.openFile(0, file);
        assertEquals(LOCKED, runChild(file));

        disk.closeFile(0);
        assertEquals(OPENED, runChild(file));
    }

    /** Starts a second JVM that runs {@link Child#main} on {@code file} and returns its exit code. */
    private static int runChild(Path file) throws IOException, InterruptedException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java,
                "-cp", System.getProperty("java.class.path"),
                Child.class.getName(), file.toString())
                .redirectErrorStream(true)
                .start();
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "child process did not finish");
        int exit = process.exitValue();
        if (exit != OPENED && exit != LOCKED) {
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new AssertionError("child failed with exit code " + exit + ":\n" + output);
        }
        return exit;
    }

    /** Runs in the child JVM: tries to open the file and reports the outcome as its exit code. */
    static final class Child {
        public static void main(String[] args) {
            try (DiskManager disk = new DiskManager()) {
                disk.openFile(0, Path.of(args[0]));
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("locked by another process")) {
                    System.exit(LOCKED);
                }
                throw e;
            }
            System.exit(OPENED);
        }
    }
}
