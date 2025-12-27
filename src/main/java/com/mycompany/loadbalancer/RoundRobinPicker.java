package com.mycompany.loadbalancer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPicker implements ContainerPicker {
    private final AtomicInteger index = new AtomicInteger(0);

    @Override
    
    public FileContainer choose(List<FileContainer> healthy) {
        if (healthy == null || healthy.isEmpty()) return null;
        
        // Use modulo to wrap around the list size
        int i = index.getAndIncrement() % healthy.size();
        return healthy.get(Math.abs(i));
    }

    @Override
    public String name() {
        return "Round Robin";
    }
}