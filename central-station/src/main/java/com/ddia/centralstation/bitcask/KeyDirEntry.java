package com.ddia.centralstation.bitcask;

/**
 * Represents an entry in the in-memory KeyDir hash map.
 * Maps a key to the location of its value in a segment file.
 */
public class KeyDirEntry {
    private final int fileId;
    private final int valueSize;
    private final long valuePos;
    private final long timestamp;

    public KeyDirEntry(int fileId, int valueSize, long valuePos, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valuePos = valuePos;
        this.timestamp = timestamp;
    }

    public int getFileId() { return fileId; }
    public int getValueSize() { return valueSize; }
    public long getValuePos() { return valuePos; }
    public long getTimestamp() { return timestamp; }
}
