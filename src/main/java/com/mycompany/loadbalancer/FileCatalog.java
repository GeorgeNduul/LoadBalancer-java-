
package com.mycompany.loadbalancer;

import java.util.*;

public class FileCatalog {
    private final Map<String, Set<FileContainer>> fileLocations = new HashMap<>();
    private volatile int replicationFactor;

    public FileCatalog(int rf) { this.replicationFactor = Math.max(1, rf); }

    public synchronized void setReplicationFactor(int rf) { this.replicationFactor = Math.max(1, rf); }
    public int getReplicationFactor() { return replicationFactor; }

    public synchronized void place(String filename, List<FileContainer> chosen) {
        fileLocations.put(filename, new HashSet<>(chosen));
    }
    public synchronized Set<FileContainer> locations(String filename) {
        return fileLocations.getOrDefault(filename, Collections.emptySet());
    }
    public synchronized void addReplica(String filename, FileContainer c) {
        fileLocations.computeIfAbsent(filename, k -> new HashSet<>()).add(c);
    }
    public synchronized void removeReplica(String filename, FileContainer c) {
        Set<FileContainer> s = fileLocations.get(filename);
        if (s != null) {
            s.remove(c);
            if (s.isEmpty()) fileLocations.remove(filename);
        }
    }
    public synchronized boolean exists(String filename) {
        Set<FileContainer> s = fileLocations.get(filename);
        return s != null && !s.isEmpty();
    }

    Object allLocations() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    Iterable<String> filesOn(FileContainer c) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
