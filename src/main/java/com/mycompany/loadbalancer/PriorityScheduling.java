
package com.mycompany.loadbalancer;


import java.util.Optional;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PriorityScheduling implements SchedulingAlgorithm {
    private final PriorityBlockingQueue<Job> pq =
        new PriorityBlockingQueue<>(11, (a,b) -> {
            int c = Integer.compare(b.priority, a.priority);
            return c != 0 ? c : Long.compare(a.arrivedAt, b.arrivedAt);
        });

    @Override public void onJobArrived(Job job) { pq.offer(job); }
    @Override public Optional<Job> nextJob() {
        try { return Optional.ofNullable(pq.poll(250, TimeUnit.MILLISECONDS)); }
        catch (InterruptedException e) { return Optional.empty(); }
    }
    @Override public void onJobCompleted(Job job) {}
    @Override public String name() { return "Priority"; }
}
