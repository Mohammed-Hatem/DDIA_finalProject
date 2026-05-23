package com.ddia.centralstation.bitcask;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * BitCask Riak key-value store implementation.
 *
 * Design:
 *   - Append-only segment files for durability
 *   - In-memory KeyDir (hash map) for O(1) lookups
 *   - Hint files for fast crash recovery
 *   - Scheduled compaction to merge old segments
 *
 * Segment file entry format:
 *   [timestamp:8 bytes][key_size:4 bytes][value_size:4 bytes][key:N bytes][value:M bytes]
 *
 * Hint file entry format:
 *   [timestamp:8 bytes][key_size:4 bytes][value_size:4 bytes][value_pos:8 bytes][key:N bytes]
 */
@Component
public class BitCask {

    private static final long MAX_SEGMENT_SIZE = 1024 * 1024; // 1 MB
    private static final String SEGMENT_PREFIX = "segment_";
    private static final String HINT_PREFIX = "hint_";
    private static final int HEADER_SIZE = 8 + 4 + 4; // timestamp + keySize + valueSize

    @Value("${bitcask.directory:./data/bitcask}")
    private String directory;

    private final ConcurrentHashMap<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private RandomAccessFile activeFile;
    private RandomAccessFile activeHintFile;
    private int activeFileId;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ScheduledExecutorService compactionScheduler;

    public BitCask() {}

    /** Constructor for non-Spring usage (e.g., testing). */
    public BitCask(String directory) {
        this.directory = directory;
        init();
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(Path.of(directory));
            recover();
            compactionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "bitcask-compaction");
                t.setDaemon(true);
                return t;
            });
            compactionScheduler.scheduleAtFixedRate(this::compact, 5, 5, TimeUnit.MINUTES);
            System.out.println("[BitCask] Initialized at " + directory
                    + " | keys loaded: " + keyDir.size());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize BitCask", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        lock.writeLock().lock();
        try {
            if (compactionScheduler != null) compactionScheduler.shutdownNow();
            if (activeFile != null) activeFile.close();
            if (activeHintFile != null) activeHintFile.close();
        } catch (IOException e) {
            System.err.println("[BitCask] Error during shutdown: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Public API

    /** Store or update a key-value pair. */
    public void put(String key, String value) {
        lock.writeLock().lock();
        try {
            ensureActiveFile();

            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.getBytes();
            long timestamp = System.currentTimeMillis();

            // Compute position of the value within the file
            long valuePos = activeFile.getFilePointer() + HEADER_SIZE + keyBytes.length;

            // Write entry
            ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + keyBytes.length + valueBytes.length);
            buf.putLong(timestamp);
            buf.putInt(keyBytes.length);
            buf.putInt(valueBytes.length);
            buf.put(keyBytes);
            buf.put(valueBytes);
            activeFile.write(buf.array());

            // Update in-memory index
            keyDir.put(key, new KeyDirEntry(activeFileId, valueBytes.length, valuePos, timestamp));

            // Append hint entry: [timestamp][key_size][value_size][value_pos][key]
            ByteBuffer hintBuf = ByteBuffer.allocate(8 + 4 + 4 + 8 + keyBytes.length);
            hintBuf.putLong(timestamp);
            hintBuf.putInt(keyBytes.length);
            hintBuf.putInt(valueBytes.length);
            hintBuf.putLong(valuePos);
            hintBuf.put(keyBytes);
            activeHintFile.write(hintBuf.array());

            // Rotate if segment exceeds max size
            if (activeFile.getFilePointer() >= MAX_SEGMENT_SIZE) {
                rotateActiveFile();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("BitCask put failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Retrieve the value for a key, or null if not found. */
    public String get(String key) {
        lock.readLock().lock();
        try {
            KeyDirEntry entry = keyDir.get(key);
            if (entry == null) return null;

            try (RandomAccessFile raf = new RandomAccessFile(segmentPath(entry.getFileId()), "r")) {
                raf.seek(entry.getValuePos());
                byte[] valueBytes = new byte[entry.getValueSize()];
                raf.readFully(valueBytes);
                return new String(valueBytes);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("BitCask get failed", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Return all key-value pairs. */
    public Map<String, String> getAll() {
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : keyDir.keySet()) {
            String value = get(key);
            if (value != null) result.put(key, value);
        }
        return result;
    }

    /** Return all keys. */
    public Set<String> keys() {
        return Collections.unmodifiableSet(keyDir.keySet());
    }

    // Active file management

    private void ensureActiveFile() throws IOException {
        if (activeFile == null) {
            activeFileId = nextFileId();
            String path = segmentPath(activeFileId);
            activeFile = new RandomAccessFile(path, "rw");
            activeFile.seek(activeFile.length());

            String hint = hintPath(activeFileId);
            activeHintFile = new RandomAccessFile(hint, "rw");
            activeHintFile.seek(activeHintFile.length());
        }
    }

    private void rotateActiveFile() throws IOException {
        if (activeFile != null) {
            activeFile.close();
            activeFile = null;
        }

        if (activeHintFile != null) {
            activeHintFile.close();
            activeHintFile = null;
        }
    }

    private int nextFileId() {
        int maxId = 0;
        File dir = new File(directory);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                String name = f.getName();
                if (name.startsWith(SEGMENT_PREFIX)) {
                    int id = Integer.parseInt(name.substring(SEGMENT_PREFIX.length()));
                    maxId = Math.max(maxId, id);
                }
            }
        }
        return maxId + 1;
    }

    // Recovery

    /**
     * Recover the in-memory keyDir from hint files (fast) and segment files.
     * Hint files are read first; segments without hint files are scanned fully.
     */
    private void recover() {
        File dir = new File(directory);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) return;

        // Collect all segment IDs
        List<Integer> segmentIds = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(SEGMENT_PREFIX))
                .map(f -> Integer.parseInt(f.getName().substring(SEGMENT_PREFIX.length())))
                .sorted()
                .collect(Collectors.toList());

        // Collect hint file IDs
        Set<Integer> hintIds = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(HINT_PREFIX))
                .map(f -> Integer.parseInt(f.getName().substring(HINT_PREFIX.length())))
                .collect(Collectors.toSet());

        for (int segId : segmentIds) {
            if (hintIds.contains(segId)) {
                recoverFromHintFile(segId);
            } else {
                recoverFromSegmentFile(segId);
            }
        }

        // The last segment file is the active file
        if (!segmentIds.isEmpty()) {
            int lastId = segmentIds.get(segmentIds.size() - 1);
            try {
                activeFileId = lastId;
                activeFile = new RandomAccessFile(segmentPath(lastId), "rw");
                activeFile.seek(activeFile.length());

                activeHintFile = new RandomAccessFile(hintPath(lastId), "rw");
                activeHintFile.seek(activeHintFile.length());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to open active file", e);
            }
        }
    }

    /** Read a hint file to quickly rebuild keyDir entries for a segment. */
    private void recoverFromHintFile(int fileId) {
        String path = hintPath(fileId);
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                long timestamp = raf.readLong();
                int keySize = raf.readInt();
                int valueSize = raf.readInt();
                long valuePos = raf.readLong();
                byte[] keyBytes = new byte[keySize];
                raf.readFully(keyBytes);
                String key = new String(keyBytes);

                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || timestamp >= existing.getTimestamp()) {
                    keyDir.put(key, new KeyDirEntry(fileId, valueSize, valuePos, timestamp));
                }
            }
        } catch (IOException e) {
            System.err.println("[BitCask] Error reading hint file " + path + ": " + e.getMessage());
        }
    }

    /** Scan a full segment file to rebuild keyDir entries. */
    private void recoverFromSegmentFile(int fileId) {
        String path = segmentPath(fileId);
        String hint = hintPath(fileId);
        try (RandomAccessFile raf = new RandomAccessFile(path, "r");
             RandomAccessFile hintFile = new RandomAccessFile(hint, "rw")) {

            // Truncate and rebuild hint file from this segment
            hintFile.setLength(0);

            while (raf.getFilePointer() < raf.length()) {
                long entryStart = raf.getFilePointer();
                long timestamp = raf.readLong();
                int keySize = raf.readInt();
                int valueSize = raf.readInt();
                byte[] keyBytes = new byte[keySize];
                raf.readFully(keyBytes);
                long valuePos = entryStart + HEADER_SIZE + keySize;
                raf.skipBytes(valueSize); // skip value data
                String key = new String(keyBytes);

                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || timestamp >= existing.getTimestamp()) {
                    keyDir.put(key, new KeyDirEntry(fileId, valueSize, valuePos, timestamp));
                }

                // Write hint entry: [timestamp][key_size][value_size][value_pos][key]
                ByteBuffer hintBuf = ByteBuffer.allocate(8 + 4 + 4 + 8 + keyBytes.length);
                hintBuf.putLong(timestamp);
                hintBuf.putInt(keyBytes.length);
                hintBuf.putInt(valueSize);
                hintBuf.putLong(valuePos);
                hintBuf.put(keyBytes);
                hintFile.write(hintBuf.array());
            }
        } catch (IOException e) {
            System.err.println("[BitCask] Error reading segment file " + path + ": " + e.getMessage());
        }
    }

    // Compaction

    /**
     * Merge old (non-active) segments into a single compacted segment.
     * Only the latest value per key is retained. A hint file is written for
     * the compacted segment, and old segments + their hints are deleted.
     * Runs under write-lock to avoid disrupting active readers.
     */
    public void compact() {
        lock.writeLock().lock();
        try {
            List<Integer> oldIds = getOldSegmentIds();
            if (oldIds.isEmpty()) return;

            System.out.println("[BitCask] Compaction started, merging " + oldIds.size() + " segments");

            // Collect latest values for keys that live in old segments
            Map<String, String> latestValues = new LinkedHashMap<>();
            for (Map.Entry<String, KeyDirEntry> e : keyDir.entrySet()) {
                if (oldIds.contains(e.getValue().getFileId())) {
                    String val = readValueFromFile(e.getValue());
                    if (val != null) latestValues.put(e.getKey(), val);
                }
            }

            if (latestValues.isEmpty()) return;

            // Write compacted segment + hint file
            int compactedId = nextFileId();
            try (RandomAccessFile segFile = new RandomAccessFile(segmentPath(compactedId), "rw");
                 RandomAccessFile hintFile = new RandomAccessFile(hintPath(compactedId), "rw")) {

                for (Map.Entry<String, String> entry : latestValues.entrySet()) {
                    byte[] keyBytes = entry.getKey().getBytes();
                    byte[] valueBytes = entry.getValue().getBytes();
                    long timestamp = System.currentTimeMillis();

                    long valuePos = segFile.getFilePointer() + HEADER_SIZE + keyBytes.length;

                    // Write to segment
                    ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + keyBytes.length + valueBytes.length);
                    buf.putLong(timestamp);
                    buf.putInt(keyBytes.length);
                    buf.putInt(valueBytes.length);
                    buf.put(keyBytes);
                    buf.put(valueBytes);
                    segFile.write(buf.array());

                    // Write to hint file
                    ByteBuffer hintBuf = ByteBuffer.allocate(8 + 4 + 4 + 8 + keyBytes.length);
                    hintBuf.putLong(timestamp);
                    hintBuf.putInt(keyBytes.length);
                    hintBuf.putInt(valueBytes.length);
                    hintBuf.putLong(valuePos);
                    hintBuf.put(keyBytes);
                    hintFile.write(hintBuf.array());

                    // Update keyDir to point to compacted segment
                    keyDir.put(entry.getKey(),
                            new KeyDirEntry(compactedId, valueBytes.length, valuePos, timestamp));
                }
            }

            // Delete old segment + hint files
            for (int oldId : oldIds) {
                Files.deleteIfExists(Path.of(segmentPath(oldId)));
                Files.deleteIfExists(Path.of(hintPath(oldId)));
            }

            System.out.println("[BitCask] Compaction done, compacted into segment_" + compactedId);
        } catch (IOException e) {
            System.err.println("[BitCask] Compaction error: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private List<Integer> getOldSegmentIds() {
        File dir = new File(directory);
        File[] files = dir.listFiles();
        if (files == null) return Collections.emptyList();

        return Arrays.stream(files)
                .filter(f -> f.getName().startsWith(SEGMENT_PREFIX))
                .map(f -> Integer.parseInt(f.getName().substring(SEGMENT_PREFIX.length())))
                .filter(id -> id != activeFileId)
                .sorted()
                .collect(Collectors.toList());
    }

    private String readValueFromFile(KeyDirEntry entry) {
        try (RandomAccessFile raf = new RandomAccessFile(segmentPath(entry.getFileId()), "r")) {
            raf.seek(entry.getValuePos());
            byte[] valueBytes = new byte[entry.getValueSize()];
            raf.readFully(valueBytes);
            return new String(valueBytes);
        } catch (IOException e) {
            return null;
        }
    }

    // Helpers

    private String segmentPath(int id) {
        return directory + File.separator + SEGMENT_PREFIX + id;
    }

    private String hintPath(int id) {
        return directory + File.separator + HINT_PREFIX + id;
    }
}
