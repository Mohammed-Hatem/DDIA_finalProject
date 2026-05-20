package com.ddia.centralstation.bitcask;

public class BitCask {
    
    // TODO: Implement the LSM tree structure
    // TODO: Implement put(key, value)
    // TODO: Implement get(key)
    // TODO: Implement hint files for recovery
    // TODO: Implement scheduled compaction to avoid disrupting active readers
    // No need to implement checksums or tombstones

    public BitCask(String directoryPath) {
        // Initialization
    }
    
    public void put(String key, String value) {
        // Write to append-only log file and update in-memory keydir
    }
    
    public String get(String key) {
        // Read from keydir and fetch from file
        return null;
    }
    
    public void compact() {
        // Merge segment files
    }
}
