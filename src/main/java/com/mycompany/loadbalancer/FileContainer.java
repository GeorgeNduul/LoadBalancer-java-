
package com.mycompany.loadbalancer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FileContainer {
    public final String id;
    public final ReentrantLock lock = new ReentrantLock(true);
    public final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    public final AtomicBoolean healthy = new AtomicBoolean(true);
    public final AtomicInteger activeOps = new AtomicInteger(0);
    public final AtomicInteger totalOps = new AtomicInteger(0);

    public FileContainer(String id) { this.id = id; }
    public boolean isHealthy() { return healthy.get(); }
}
