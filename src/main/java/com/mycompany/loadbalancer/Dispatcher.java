
package com.mycompany.loadbalancer;

import java.io.FileNotFoundException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Dispatcher {
    private volatile SchedulingAlgorithm scheduler;
    private final ContainerPicker picker;
    private final List<FileContainer> containers;
    private final FileCatalog catalog;
    private final long lockTimeoutMs;

    private final ExecutorService runnerPool = Executors.newCachedThreadPool();
    private final AtomicInteger jobsInFlight = new AtomicInteger(0);
    private final AtomicInteger jobsCompleted = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);
    private volatile boolean running = true;

    void addListener(MqttGateway aThis) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    


public interface JobEventListener {
    void onQueued(Job job);
    void onStarted(Job job);
    void onCompleted(Job job);
    void onFailed(Job job, Throwable error);
}

    

    public Dispatcher(SchedulingAlgorithm scheduler, ContainerPicker picker,
                      List<FileContainer> containers, FileCatalog catalog, long lockTimeoutMs) {
        this.scheduler = scheduler != null ? scheduler : new FCFS();
        this.picker = picker;
        this.containers = containers;
        this.catalog = catalog;
        this.lockTimeoutMs = lockTimeoutMs;
    }

    public void start() {
        Thread t = new Thread(this::loop, "dispatcher");
        t.setDaemon(true); t.start();
    }
    public void stop() { running = false; runnerPool.shutdownNow(); }

    public void submit(Job job) { scheduler.onJobArrived(job); }
    public void setScheduler(SchedulingAlgorithm s) { this.scheduler = s; }

    private void loop() {
        while (running) {
            Optional<Job> maybe = scheduler.nextJob();
            if (!maybe.isPresent()) { try { Thread.sleep(50); } catch (InterruptedException ignored) {} continue; }
            Job job = maybe.get();
            jobsInFlight.incrementAndGet();
            runnerPool.submit(() -> {
                try { execute(job); scheduler.onJobCompleted(job); jobsCompleted.incrementAndGet(); }
                catch (Exception e) { jobsFailed.incrementAndGet(); System.err.println("Job fail " + job.id + ": " + e.getMessage()); }
                finally { jobsInFlight.decrementAndGet(); }
            });
        }
    }

    private List<FileContainer> healthyContainers() {
        return containers.stream().filter(FileContainer::isHealthy).collect(Collectors.toList());
    }

    private void execute(Job job) throws Exception {
        switch (job.type) {
            case UPLOAD: handleUpload(job); break;
            case DOWNLOAD: handleDownload(job); break;
            case DELETE: handleDelete(job); break;
        }
    }

    private void handleUpload(Job job) throws Exception {
        List<FileContainer> healthy = healthyContainers();
        if (healthy.isEmpty()) throw new IllegalStateException("No healthy containers");

        int rf = Math.min(catalog.getReplicationFactor(), healthy.size());
        Set<FileContainer> chosen = new HashSet<>();
        for (int i = 0; i < rf; i++) {
            FileContainer c;
            int attempts = 0;
            do { c = picker.choose(healthy); attempts++; } while (c != null && chosen.contains(c) && attempts < healthy.size());
            if (c == null) c = healthy.get(i % healthy.size());
            chosen.add(c);
        }

        for (FileContainer c : chosen) {
            boolean ok = c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS);
            if (!ok) throw new TimeoutException("Lock timeout " + c.id);
            try {
                c.activeOps.incrementAndGet();
                simulateDelay(job);
                c.storage.put(job.filename, job.payload != null ? job.payload : fakeContent(job.sizeKB));
                c.totalOps.incrementAndGet();
                catalog.addReplica(job.filename, c);
            } finally {
                c.activeOps.decrementAndGet();
                c.lock.unlock();
            }
        }
        catalog.place(job.filename, new ArrayList<>(chosen));
    }

    private void handleDownload(Job job) throws Exception {
        Set<FileContainer> locs = catalog.locations(job.filename);
        if (locs.isEmpty()) throw new FileNotFoundException("Not found " + job.filename);
        FileContainer c = locs.stream().filter(FileContainer::isHealthy).findFirst()
                .orElseThrow(() -> new IllegalStateException("No healthy replicas"));

        boolean ok = c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS);
        if (!ok) throw new TimeoutException("Lock timeout " + c.id);
        try {
            c.activeOps.incrementAndGet();
            simulateDelay(job);
            byte[] data = c.storage.get(job.filename);
            if (data == null) throw new FileNotFoundException("Missing data on " + c.id);
            // Normally return data via HTTP; here we just simulate.
        } finally {
            c.activeOps.decrementAndGet();
            c.lock.unlock();
        }
    }

    private void handleDelete(Job job) throws Exception {
        Set<FileContainer> locs = catalog.locations(job.filename);
        if (locs.isEmpty()) return;
        for (FileContainer c : locs) {
            if (!c.isHealthy()) continue;
            if (c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    c.activeOps.incrementAndGet();
                    simulateDelay(job);
                    c.storage.remove(job.filename);
                    c.totalOps.incrementAndGet();
                } finally {
                    c.activeOps.decrementAndGet();
                    c.lock.unlock();
                }
            }
        }
        catalog.removeReplica(job.filename, null); // optional; you can remove all via loop
    }

    private void simulateDelay(Job job) throws InterruptedException {
        long ms = Math.max(1000, Math.min(5000, job.estimatedMs));
        Thread.sleep(ms);
    }
    private byte[] fakeContent(int sizeKB) {
        byte[] data = new byte[sizeKB * 1024]; Arrays.fill(data, (byte) 'x'); return data;
    }

    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobsInFlight", jobsInFlight.get());
        m.put("jobsCompleted", jobsCompleted.get());
        m.put("jobsFailed", jobsFailed.get());
        return m;
    }
}
