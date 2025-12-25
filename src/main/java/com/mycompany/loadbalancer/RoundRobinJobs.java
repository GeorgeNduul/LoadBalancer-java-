
package com.mycompany.loadbalancer;


import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RoundRobinJobs implements SchedulingAlgorithm {
    private final Map<JobType, Queue<Job>> buckets = new EnumMap<>(JobType.class);
    private final JobType[] order = JobType.values();
    private int idx = 0;

    public RoundRobinJobs() { for (JobType t : JobType.values()) buckets.put(t, new ConcurrentLinkedQueue<>()); }

    @Override public void onJobArrived(Job job) { buckets.get(job.type).offer(job); }
    @Override public Optional<Job> nextJob() {
        for (int i = 0; i < order.length; i++) {
            int j = (idx + i) % order.length;
            JobType type = order[j];
            Job job = buckets.get(type).poll();
            if (job != null) { idx = (j + 1) % order.length; return Optional.of(job); }
        }
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        return Optional.empty();
    }
    @Override public void onJobCompleted(Job job) {}
    @Override public String name() { return "Round-Robin-Jobs"; }
}
