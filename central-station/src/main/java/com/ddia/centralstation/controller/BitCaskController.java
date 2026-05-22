package com.ddia.centralstation.controller;

import com.ddia.centralstation.bitcask.BitCask;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller exposing BitCask data.
 * Used by the bitcask_client.sh script to query station statuses.
 */
@RestController
@RequestMapping("/api/bitcask")
public class BitCaskController {

    private final BitCask bitCask;

    public BitCaskController(BitCask bitCask) {
        this.bitCask = bitCask;
    }

    /** Get all keys and their latest values. */
    @GetMapping("/all")
    public ResponseEntity<Map<String, String>> getAll() {
        return ResponseEntity.ok(bitCask.getAll());
    }

    /** Get the value for a specific key (raw JSON string stored in BitCask). */
    @GetMapping(value = "/key/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getByKey(@PathVariable("key") String key) {
        String value = bitCask.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(value);
    }

    /** Trigger compaction manually (for testing). */
    @PostMapping("/compact")
    public ResponseEntity<String> compact() {
        bitCask.compact();
        return ResponseEntity.ok("Compaction triggered.");
    }
}
