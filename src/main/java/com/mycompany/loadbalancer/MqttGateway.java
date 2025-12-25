
// lb/integration/MqttGateway.java
package com.mycompany.loadbalancer;

import com.mycompany.loadbalancer.Dispatcher.JobEventListener;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttGateway implements JobEventListener {
    // Topics
    public static final String GUI_COMMANDS = "lb/gui/commands";
    public static final String GUI_ACKS     = "lb/gui/acks";
    public static final String GUI_RESULTS_PREFIX = "lb/gui/results/"; // + jobId

    // Optional: external aggregator topic if you want to forward requests via MQTT
    public static final String AGGREGATOR_COMMANDS = "lb/aggregator/commands";

    private final MqttClient client;
    private final Dispatcher dispatcher;
    private final UserService users;
    private final boolean forwardToExternalAggregator; // true → publish to AGGREGATOR_COMMANDS; false → submit to Dispatcher
    private final int qos = 1;

    public MqttGateway(String brokerUrl,
                       String clientId,
                       Dispatcher dispatcher,
                       UserService users,
                       boolean forwardToExternalAggregator) throws MqttException {

        this.dispatcher = dispatcher;
        this.users = users;
        this.forwardToExternalAggregator = forwardToExternalAggregator;

        this.client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        // Last Will & Testament (optional): notify GUI that LB went offline
        opts.setWill(GUI_ACKS, "{\"status\":\"LB_OFFLINE\"}".getBytes(StandardCharsets.UTF_8), qos, false);

        client.setCallback(new MqttCallback() {
            @Override public void connectionLost(Throwable cause) {
                System.err.println("[MQTT] Connection lost: " + cause);
            }
            @Override public void messageArrived(String topic, MqttMessage message) throws Exception {
                handleIncoming(topic, message);
            }
            @Override public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        client.connect(opts);

        // Subscribe to GUI commands
        client.subscribe(GUI_COMMANDS, qos);

        // Listen to Dispatcher events
        dispatcher.addListener(this);

        System.out.println("[MQTT] Connected to " + brokerUrl + ", subscribed to " + GUI_COMMANDS);
    }

    private void handleIncoming(String topic, MqttMessage msg) {
        String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
        try {
            Map<String, Object> json = Json.parse(payload);
            String typeStr = (String) json.get("type");
            String user = (String) json.get("user");
            String pass = (String) json.getOrDefault("pass", null);
            String filename = (String) json.get("filename");
            int priority = Json.intOr(json.get("priority"), 5);
            int sizeKB = Json.intOr(json.get("sizeKB"), 64);
            String dataB64 = (String) json.getOrDefault("dataBase64", null);

            var u = users.auth(user, pass);
            if (u == null) {
                publish(GUI_ACKS, Json.stringify(Map.of(
                        "status", "UNAUTHORIZED",
                        "user", user,
                        "filename", filename
                )));
                return;
            }

            JobType type = JobType.valueOf(typeStr.toUpperCase());
            byte[] data = (dataB64 != null) ? Base64.getDecoder().decode(dataB64) : null;
            Job job = new Job(type, u.name, filename, data, sizeKB, priority);

            if (forwardToExternalAggregator) {
                // Forward to external aggregator topic (as-is or normalized)
                publish(AGGREGATOR_COMMANDS, payload);
                publish(GUI_ACKS, Json.stringify(Map.of(
                        "jobId", job.id, "status", "FORWARDED", "type", type.name(), "filename", filename
                )));
            } else {
                // Submit to in-process Dispatcher
                dispatcher.submit(job);
                publish(GUI_ACKS, Json.stringify(Map.of(
                        "jobId", job.id, "status", "QUEUED", "type", type.name(), "filename", filename
                )));
            }

        } catch (Exception e) {
            System.err.println("[MQTT] Failed to process message: " + e.getMessage());
            publish(GUI_ACKS, Json.stringify(Map.of("status", "BAD_REQUEST", "error", e.getMessage())));
        }
    }

    // Publish helpers
    private void publish(String topic, String payload) {
        try {
            client.publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, false);
        } catch (MqttException e) {
            System.err.println("[MQTT] Publish failed to " + topic + ": " + e.getMessage());
        }
    }
    private void publishResult(Job job, String status, String message) {
        String topic = GUI_RESULTS_PREFIX + job.id;
        publish(topic, Json.stringify(Map.of(
                "jobId", job.id,
                "status", status,
                "type", job.type.name(),
                "filename", job.filename,
                "message", message
        )));
    }

    // Dispatcher event callbacks
    @Override public void onQueued(Job job) {
        publish(GUI_ACKS, Json.stringify(Map.of("jobId", job.id, "status", "QUEUED", "type", job.type.name(), "filename", job.filename)));
    }
    @Override public void onStarted(Job job) { publishResult(job, "STARTED", ""); }
    @Override public void onCompleted(Job job) { publishResult(job, "COMPLETED", ""); }
    @Override public void onFailed(Job job, Throwable error) { publishResult(job, "FAILED", error.getMessage()); }

    // Simple JSON helper (no external libs; minimal)
    static class Json {
        // NOTE: For coursework, a tiny parser is OK. For production, use Jackson/Gson.
        @SuppressWarnings("unchecked")
        static Map<String,Object> parse(String s) {
            // Extremely simplified parser expecting flat JSON:
            java.util.Map<String,Object> map = new java.util.LinkedHashMap<>();
            s = s.trim();
            if (s.startsWith("{") && s.endsWith("}")) s = s.substring(1, s.length()-1);
            for (String part : s.split(",")) {
                int i = part.indexOf(':'); if (i < 0) continue;
                String k = part.substring(0,i).trim().replaceAll("^\"|\"$", "");
                String v = part.substring(i+1).trim();
                if (v.startsWith("\"")) map.put(k, v.replaceAll("^\"|\"$", ""));
                else if ("true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v)) map.put(k, Boolean.parseBoolean(v));
                else {
                    try { map.put(k, Integer.parseInt(v)); }
                    catch (Exception e) { map.put(k, v); }
                }
            }
            return map;
        }
        static int intOr(Object o, int def) {
            try { return (o instanceof Number) ? ((Number)o).intValue() : Integer.parseInt(String.valueOf(o)); }
            catch (Exception e) { return def; }
        }
        static String stringify(Map<String,Object> m) {
            StringBuilder sb = new StringBuilder("{"); boolean first = true;
            for (Map.Entry<String,Object> e : m.entrySet()) {
                if (!first) sb.append(','); first = false;
                sb.append('"').append(e.getKey()).append('"').append(':').append(value(e.getValue()));
            }
            sb.append('}'); return sb.toString();
        }
        static String value(Object v) {
            if (v == null) return "null";
            if (v instanceof Number || v instanceof Boolean) return v.toString();
            return '"' + v.toString().replace("\"","\\\"") + '"';
        }
    }
}
