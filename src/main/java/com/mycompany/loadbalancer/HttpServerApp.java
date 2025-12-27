package com.mycompany.loadbalancer;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import static java.lang.Integer.parseInt;

public class HttpServerApp {
    private final UserService users;
    private final Dispatcher dispatcher;
    private final FileCatalog catalog;
    private final List<FileContainer> containers;

    public HttpServerApp(UserService users, Dispatcher dispatcher, FileCatalog catalog, List<FileContainer> containers) {
        this.users = users; 
        this.dispatcher = dispatcher; 
        this.catalog = catalog; 
        this.containers = containers;
    }

    public void start(int port) throws IOException {
        // FIX: Use the 'port' variable passed from Main.java instead of a hardcoded 8081
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        
        // --- Context Mappings ---
        server.createContext("/upload", this::upload);
        server.createContext("/download", this::download);
        server.createContext("/delete", this::delete);
        server.createContext("/share", this::share);
        server.createContext("/user/create", this::userCreate);
        server.createContext("/user/update", this::userUpdate);
        server.createContext("/admin/user/delete", this::adminUserDelete);
        server.createContext("/admin/user/promote", this::adminPromote);
        server.createContext("/admin/addContainer", this::addContainer);
        server.createContext("/admin/removeContainer", this::removeContainer);
        server.createContext("/admin/setHealth", this::setHealth);
        server.createContext("/admin/setReplication", this::setReplication);
        server.createContext("/admin/setScheduler", this::setScheduler);
        server.createContext("/metrics", this::metrics);
        server.createContext("/", ex -> respondText(ex, 200, "LB running"));

        // Optimized Thread Pool for handling multiple API requests
        server.setExecutor(Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors())));
        
        server.start();
        System.out.println("[HTTP] API listening on http://localhost:" + port);
    }

    // --- Core Handlers ---

    private void upload(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use POST"); return; }
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass"));
        if (u == null) { respondText(ex, 401, "Unauthorized"); return; }
        
        String filename = query(ex, "filename");
        int sizeKB = parseInt(query(ex, "sizeKB"), 64);
        int priority = parseInt(query(ex, "priority"), 5);
        
        if (filename == null) { respondText(ex, 400, "filename required"); return; }
        if (!users.canWrite(u.name, filename)) {
            if (!users.isOwner(u.name, filename)) { respondText(ex, 403, "Write denied"); return; }
        }
        
        byte[] body = readAll(ex.getRequestBody());
        Job job = new Job(JobType.UPLOAD, u.name, filename, body, sizeKB, priority);
        dispatcher.submit(job);
        respondText(ex, 202, "Upload queued: " + job.id);
    }

    private void download(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use GET"); return; }
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass"));
        if (u == null) { respondText(ex, 401, "Unauthorized"); return; }
        
        String filename = query(ex, "filename"); 
        int priority = parseInt(query(ex, "priority"), 5);
        
        if (filename == null) { respondText(ex, 400, "filename required"); return; }
        if (!users.canRead(u.name, filename)) { respondText(ex, 403, "Read denied"); return; }
        if (!catalog.exists(filename)) { respondText(ex, 404, "Not found"); return; }
        
        Job job = new Job(JobType.DOWNLOAD, u.name, filename, null, 1, priority);
        dispatcher.submit(job);
        respondText(ex, 202, "Download queued: " + job.id);
    }

    private void delete(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use POST"); return; }
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass"));
        if (u == null) { respondText(ex, 401, "Unauthorized"); return; }
        
        String filename = query(ex, "filename"); 
        int priority = parseInt(query(ex, "priority"), 5);
        
        if (filename == null) { respondText(ex, 400, "filename required"); return; }
        if (!users.canWrite(u.name, filename)) { respondText(ex, 403, "Write denied"); return; }
        if (!catalog.exists(filename)) { respondText(ex, 404, "Not found"); return; }
        
        Job job = new Job(JobType.DELETE, u.name, filename, null, 1, priority);
        dispatcher.submit(job);
        respondText(ex, 202, "Delete queued: " + job.id);
    }

    private void share(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use POST"); return; }
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass"));
        if (u == null) { respondText(ex, 401, "Unauthorized"); return; }
        
        String filename = query(ex, "filename"); 
        String to = query(ex, "to"); 
        String perm = query(ex, "perm");
        
        if (filename == null || to == null || perm == null) { respondText(ex, 400, "filename,to,perm required"); return; }
        
        UserService.Perm p = "write".equalsIgnoreCase(perm) ? UserService.Perm.WRITE : UserService.Perm.READ;
        boolean ok = users.share(u.name, to, filename, p);
        respondText(ex, ok ? 200 : 403, ok ? "Shared" : "Share failed");
    }

    // --- User Management ---

    private void userCreate(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use POST"); return; }
        String name = query(ex, "name"), pass = query(ex, "pass"), roleStr = query(ex, "role");
        User.Role role = "admin".equalsIgnoreCase(roleStr) ? User.Role.ADMIN : User.Role.STANDARD;
        boolean ok = users.createUser(name, pass, role);
        respondText(ex, ok ? 201 : 409, ok ? "Created" : "Exists");
    }

    private void userUpdate(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { respondText(ex, 405, "Use POST"); return; }
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null) { respondText(ex, 401, "Unauthorized"); return; }
        
        String pass = query(ex, "pass"); String roleStr = query(ex, "role");
        User.Role role = "admin".equalsIgnoreCase(roleStr) ? User.Role.ADMIN : null;
        boolean ok = users.updateUser(u.name, pass, role);
        respondText(ex, ok ? 200 : 404, ok ? "Updated" : "Not found");
    }

    private void adminUserDelete(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        String name = query(ex, "name");
        boolean ok = users.deleteUser(name);
        respondText(ex, ok ? 200 : 404, ok ? "Deleted" : "Not found");
    }

    private void adminPromote(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        String name = query(ex, "name");
        boolean ok = users.promoteToAdmin(name);
        respondText(ex, ok ? 200 : 404, ok ? "Promoted" : "Not found");
    }

    // --- Admin/Container Controls ---

    private void addContainer(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        String id = query(ex, "id"); 
        if (id == null) { respondText(ex, 400, "id required"); return; }
        containers.add(new FileContainer(id));
        respondText(ex, 201, "Container added: " + id);
    }
    
    private void removeContainer(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass"));
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }

        String id = query(ex, "id");
        if (id == null) { respondText(ex, 400, "id required"); return; }

        FileContainer c = containers.stream().filter(x -> x.id.equals(id)).findFirst().orElse(null);
        if (c == null) { respondText(ex, 404, "Not found"); return; }
        
        containers.remove(c);
        synchronized (catalog) {
            for (String f : catalog.filesOn(c)) {
                catalog.removeReplica(f, c);
            }
        }
        respondText(ex, 200, "Container removed: " + id);
    }

    private void setHealth(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        String id = query(ex, "id"); String alive = query(ex, "alive");
        FileContainer c = containers.stream().filter(x -> x.id.equals(id)).findFirst().orElse(null);
        if (c == null) { respondText(ex, 404, "Not found"); return; }
        c.healthy.set(Boolean.parseBoolean(alive));
        respondText(ex, 200, "Container " + id + " healthy=" + c.healthy.get());
    }

    private void setReplication(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        int rf = parseInt(query(ex, "rf"), 2);
        catalog.setReplicationFactor(rf);
        respondText(ex, 200, "Replication=" + rf);
    }

    private void setScheduler(HttpExchange ex) throws IOException {
        User u = users.auth(header(ex, "X-User"), header(ex, "X-Pass")); 
        if (u == null || u.role != User.Role.ADMIN) { respondText(ex, 401, "Admin required"); return; }
        String name = query(ex, "name");
        SchedulingAlgorithm alg;
        switch (name.toLowerCase()) {
            case "fcfs": alg = new FCFS(); break;
            case "sjn": alg = new ShortestJobNext(); break;
            case "priority": alg = new PriorityScheduling(); break;
            case "rr": alg = new RoundRobinJobs(); break;
            case "mlq": alg = new MultiLevelQueues(); break;
            default: respondText(ex, 400, "Unknown scheduler"); return;
        }
        dispatcher.setScheduler(alg);
        respondText(ex, 200, "Scheduler=" + alg.name());
    }

    private void metrics(HttpExchange ex) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("dispatcher", dispatcher.metrics());
        m.put("containers", containersInfo());
        m.put("replication", catalog.getReplicationFactor());
        respondJson(ex, 200, m);
    }

    // --- Global Helpers ---

    private static String header(HttpExchange ex, String k) {
        List<String> vals = ex.getRequestHeaders().get(k); 
        return (vals == null || vals.isEmpty()) ? null : vals.get(0);
    }

    private static String query(HttpExchange ex, String key) {
        String q = ex.getRequestURI().getRawQuery(); 
        if (q == null) return null;
        for (String part : q.split("&")) {
            int i = part.indexOf('='); 
            if (i > 0) {
                String k = decode(part.substring(0,i)), v = decode(part.substring(i+1));
                if (k.equals(key)) return v;
            }
        }
        return null;
    }

    private static String decode(String s) { try { return java.net.URLDecoder.decode(s, "UTF-8"); } catch (Exception e) { return s; } }

    private static byte[] readAll(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
        byte[] buf = new byte[8192]; 
        int r;
        while ((r = is.read(buf)) != -1) baos.write(buf, 0, r);
        return baos.toByteArray();
    }

    private static void respondText(HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private static void respondJson(HttpExchange ex, int status, Map<String, Object> map) throws IOException {
        String json = toJson(map);
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private static String toJson(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        if (v instanceof Collection) {
            StringBuilder sb = new StringBuilder("["); 
            boolean first = true;
            for (Object o : (Collection<?>) v) { 
                if (!first) sb.append(','); 
                first = false; 
                sb.append(toJson(o)); 
            }
            return sb.append(']').toString();
        }
        if (v instanceof Map) {
            StringBuilder sb = new StringBuilder("{"); 
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) sb.append(','); 
                first = false;
                sb.append('"').append(e.getKey()).append('"').append(':').append(toJson(e.getValue()));
            }
            return sb.append('}').toString();
        }
        return '"' + v.toString().replace("\"","\\\"") + '"';
    }

    private List<String> containersInfo() {
        List<String> info = new ArrayList<>();
        for (FileContainer c : containers) {
            info.add(String.format("%s(healthy=%s,active=%d,total=%d,files=%d)",
                    c.id, c.healthy.get(), c.activeOps.get(), c.totalOps.get(), c.storage.size()));
        }
        return info;
    }
}