
package com.mycompany.loadbalancer;


import java.util.*;

public class ShortestJobNext implements SchedulingAlgorithm {
    private final List<Job> list = Collections.synchronizedList(new ArrayList<>());

    @Override public void onJobArrived(Job job) { list.add(job); }
    @Override public Optional<Job> nextJob() {
        synchronized (list) {
            if (list.isEmpty()) return Optional.empty();
            Job best = list.stream().min(Comparator.comparingInt(j -> j.sizeKB)).orElse(null);
            if (best != null) list.remove(best);
            return Optional.ofNullable(best);
        }
    }
    @Override public void onJobCompleted(Job job) {}
    @Override public String name() { return "Shortest-Job-Next"; }
}
