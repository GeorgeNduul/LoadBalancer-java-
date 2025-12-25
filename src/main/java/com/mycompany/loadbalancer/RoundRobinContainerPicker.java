
package com.mycompany.loadbalancer;


import java.util.List;

public class RoundRobinContainerPicker implements ContainerPicker {
    private int idx = 0;
    @Override public synchronized FileContainer choose(List<FileContainer> healthy) {
        if (healthy.isEmpty()) return null;
        FileContainer c = healthy.get(idx % healthy.size());
        idx = (idx + 1) % healthy.size();
        return c;
    }
    @Override public String name() { return "Round-Robin-Containers"; }
}
