
package com.mycompany.loadbalancer;

import java.util.UUID;

public class Job {
    public final String id = UUID.randomUUID().toString();
    public final JobType type;
    public final String user;
    public final String filename;
    public final byte[] payload;    // for uploads
    public final int sizeKB;        // for SJN
    public final int priority;      // for Priority / MLQ
    public final long arrivedAt = System.currentTimeMillis();
    public volatile long estimatedMs; // used for SJN/SRTF

    public Job(JobType type, String user, String filename, byte[] payload,
               int sizeKB, int priority) {
        this.type = type; this.user = user; this.filename = filename;
        this.payload = payload; this.sizeKB = Math.max(1, sizeKB);
        this.priority = priority;
        long baseDelay = 1000 + (int) (Math.random() * 4001);
        long sizeDelay = Math.round(this.sizeKB * 0.5);
        this.estimatedMs = baseDelay + sizeDelay;
    }
}
