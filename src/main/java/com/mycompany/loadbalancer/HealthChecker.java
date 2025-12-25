
package com.mycompany.loadbalancer;

import java.util.List;
import java.util.concurrent.*;

public class HealthChecker {
    private final List<FileContainer> containers;
    private final long intervalMs;
    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "health-checker"); t.setDaemon(true); return t;
    });

    public HealthChecker(List<FileContainer> containers, long intervalMs) {
        this.containers = containers; this.intervalMs = intervalMs;
    }

    public void start() { exec.scheduleAtFixedRate(this::probeAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS); }
    private void probeAll() {
        // For simulation, we keep state; you can add random flapping or external checks
    }
}
