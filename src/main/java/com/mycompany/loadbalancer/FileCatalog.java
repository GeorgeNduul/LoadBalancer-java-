package com.mycompany.loadbalancer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileCatalog {
    // We use a Set because Dispatcher expects unique locations for files
    private final Map<String, Set<FileContainer>> mapping = new ConcurrentHashMap<>();
    private int replicationFactor = 2;

    public FileCatalog() {
        // Constructor is empty and ready
    }

    // Matches line 115 in Dispatcher (accepting an ArrayList but storing as Set)
    public void place(String filename, ArrayList<FileContainer> targets) {
        mapping.put(filename, new HashSet<>(targets));
    }

    // FIX for the "incompatible types" errors (Lines 119 and 139)
    // Returns a Set instead of a List
    public Set<FileContainer> locations(String filename) {
        return mapping.getOrDefault(filename, Collections.emptySet());
    }

    public boolean exists(String filename) {
        return mapping.containsKey(filename);
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int rf) {
        this.replicationFactor = rf;
    }

    // Helper for removing containers
    public Set<String> filesOn(FileContainer c) {
        Set<String> files = new HashSet<>();
        mapping.forEach((file, containers) -> {
            if (containers.contains(c)) files.add(file);
        });
        return files;
    }

    public void removeReplica(String filename, FileContainer container) {
        Set<FileContainer> set = mapping.get(filename);
        if (set != null) {
            set.remove(container);
            if (set.isEmpty()) mapping.remove(filename);
        }
    }
}