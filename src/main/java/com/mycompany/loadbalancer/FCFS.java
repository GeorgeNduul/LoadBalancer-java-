
package com.mycompany.loadbalancer;


import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class FCFS implements SchedulingAlgorithm {
    private final BlockingQueue<Job> q = new LinkedBlockingQueue<>();

    @Override public void onJobArrived(Job job) { q.offer(job); }
    @Override public Optional<Job> nextJob() {
        try { return Optional.ofNullable(q.poll(250, TimeUnit.MILLISECONDS)); }
        catch (InterruptedException e) { return Optional.empty(); }
    }
    @Override public void onJobCompleted(Job job) {}
    @Override public String name() { return "FCFS"; }
}
