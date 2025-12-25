
package com.mycompany.loadbalancer;

import java.util.Optional;

public interface SchedulingAlgorithm {
    void onJobArrived(Job job);
    Optional<Job> nextJob();
    void onJobCompleted(Job job);
    String name();
}
