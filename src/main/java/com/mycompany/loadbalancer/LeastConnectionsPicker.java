
package com.mycompany.loadbalancer;


import java.util.Comparator;
import java.util.List;

public class LeastConnectionsPicker implements ContainerPicker {
    @Override public FileContainer choose(List<FileContainer> healthy) {
        return healthy.stream().min(Comparator.comparingInt(c -> c.activeOps.get())).orElse(null);
    }
    @Override public String name() { return "Least-Connections"; }
}
