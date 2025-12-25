
package com.mycompany.loadbalancer;


import java.util.Optional;

/**
 *
 * @author ntu-user
 */
public class MultiLevelQueues implements SchedulingAlgorithm {
    private final SchedulingAlgorithm high = new ShortestJobNext(); // favor short jobs
    private final SchedulingAlgorithm normal = new RoundRobinJobs();
    private final SchedulingAlgorithm low = new FCFS();

    @Override public void onJobArrived(Job job) {
        if (job.priority >= 8) high.onJobArrived(job);
        else if (job.priority >= 4) normal.onJobArrived(job);
        else low.onJobArrived(job);
    }
    @Override public Optional<Job> nextJob() {
        Optional<Job> j = high.nextJob(); if (j.isPresent()) return j;
        j = normal.nextJob(); if (j.isPresent()) return j;
        return low.nextJob();
    }
    @Override public void onJobCompleted(Job job) {}
    @Override public String name() { return "Multi-Level-Queues"; }
}
