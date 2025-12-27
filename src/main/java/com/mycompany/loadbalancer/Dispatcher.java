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
    private final List<JobEventListener> listeners = new CopyOnWriteArrayList<>();
    
    private final AtomicInteger jobsInFlight = new AtomicInteger(0);
    private final AtomicInteger jobsCompleted = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);
    private volatile boolean running = true;

    // --- Interface for Events ---
    public interface JobEventListener {
        void onQueued(Job job);
        void onStarted(Job job);
        void onCompleted(Job job);
        void onFailed(Job job, Throwable error);
    }

    // --- Unified Constructor ---
    // This matches what Main.java needs: Dispatcher(catalog, containers)
    public Dispatcher(FileCatalog catalog, List<FileContainer> containers) {
        this.catalog = catalog;
        this.containers = containers;
        this.scheduler = new FCFS(); // Default to First-Come-First-Served
        this.picker = new RoundRobinPicker(); // Default picker
        this.lockTimeoutMs = 5000; // Default 5 second timeout
        
        this.start(); // Start the background loop immediately
    }

    public void addListener(JobEventListener listener) {
        this.listeners.add(listener);
    }

    public void start() {
        Thread t = new Thread(this::loop, "dispatcher-loop");
        t.setDaemon(true); 
        t.start();
        System.out.println("[Dispatcher] Background loop started.");
    }

    public void stop() { 
        running = false; 
        runnerPool.shutdownNow(); 
    }

    public void submit(Job job) { 
        scheduler.onJobArrived(job); 
        listeners.forEach(l -> l.onQueued(job));
    }

    public void setScheduler(SchedulingAlgorithm s) { 
        this.scheduler = s; 
    }

    private void loop() {
        while (running) {
            Optional<Job> maybe = scheduler.nextJob();
            if (maybe.isEmpty()) { 
                try { Thread.sleep(100); } catch (InterruptedException ignored) {} 
                continue; 
            }
            
            Job job = maybe.get();
            jobsInFlight.incrementAndGet();
            
            runnerPool.submit(() -> {
                try {
                    listeners.forEach(l -> l.onStarted(job));
                    execute(job);
                    scheduler.onJobCompleted(job);
                    jobsCompleted.incrementAndGet();
                    listeners.forEach(l -> l.onCompleted(job));
                } catch (Exception e) {
                    jobsFailed.incrementAndGet();
                    System.err.println("Job failed " + job.id + ": " + e.getMessage());
                    listeners.forEach(l -> l.onFailed(job, e));
                } finally {
                    jobsInFlight.decrementAndGet();
                }
            });
        }
    }

    private List<FileContainer> healthyContainers() {
        return containers.stream()
                .filter(FileContainer::isHealthy)
                .collect(Collectors.toList());
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
        if (healthy.isEmpty()) throw new IllegalStateException("No healthy containers available.");

        int rf = Math.min(catalog.getReplicationFactor(), healthy.size());
        ArrayList<FileContainer> chosen = new ArrayList<>();
        
        // Simple selection: Round robin or Picker logic
        for (int i = 0; i < rf; i++) {
            chosen.add(healthy.get(i % healthy.size()));
        }

        for (FileContainer c : chosen) {
            if (!c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) 
                throw new TimeoutException("Timeout acquiring lock for " + c.id);
            try {
                c.activeOps.incrementAndGet();
                simulateDelay(job);
                c.storage.put(job.filename, job.payload != null ? job.payload : fakeContent(job.sizeKB));
                c.totalOps.incrementAndGet();
            } finally {
                c.activeOps.decrementAndGet();
                c.lock.unlock();
            }
        }
        catalog.place(job.filename, chosen);
    }

    private void handleDownload(Job job) throws Exception {
        Set<FileContainer> locs = catalog.locations(job.filename);
        if (locs.isEmpty()) throw new FileNotFoundException("File not in catalog: " + job.filename);
        
        FileContainer c = locs.stream()
                .filter(FileContainer::isHealthy)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("All replicas are offline for " + job.filename));

        if (!c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) 
            throw new TimeoutException("Lock timeout on " + c.id);
        try {
            c.activeOps.incrementAndGet();
            simulateDelay(job);
            // In a real system, data would be streamed here
        } finally {
            c.activeOps.decrementAndGet();
            c.lock.unlock();
        }
    }

    private void handleDelete(Job job) throws Exception {
        Set<FileContainer> locs = catalog.locations(job.filename);
        for (FileContainer c : locs) {
            if (!c.isHealthy()) continue;
            if (c.lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    c.activeOps.incrementAndGet();
                    c.storage.remove(job.filename);
                    c.totalOps.incrementAndGet();
                } finally {
                    c.activeOps.decrementAndGet();
                    c.lock.unlock();
                }
            }
        }
        // Remove from catalog completely
        locs.forEach(c -> catalog.removeReplica(job.filename, c));
    }

    private void simulateDelay(Job job) throws InterruptedException {
        // Base delay on size: 10ms per KB, min 100ms
        long ms = Math.max(100, job.sizeKB * 10L); 
        Thread.sleep(ms);
    }

    private byte[] fakeContent(int sizeKB) {
        byte[] data = new byte[sizeKB * 1024]; 
        Arrays.fill(data, (byte) 'x'); 
        return data;
    }

    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobsInFlight", jobsInFlight.get());
        m.put("jobsCompleted", jobsCompleted.get());
        m.put("jobsFailed", jobsFailed.get());
        m.put("scheduler", scheduler != null ? scheduler.getClass().getSimpleName() : "None");
        return m;
    }
}