package com.mycompany.loadbalancer;

import com.mycompany.loadbalancer.Dispatcher.JobEventListener;
import org.eclipse.paho.client.mqttv3.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttGateway implements JobEventListener {
    public static final String GUI_COMMANDS = "lb/gui/commands";
    public static final String GUI_ACKS     = "lb/gui/acks";
    public static final String GUI_RESULTS_PREFIX = "lb/gui/results/";
    public static final String AGGREGATOR_COMMANDS = "lb/aggregator/commands";

    private final MqttClient client;
    private final Dispatcher dispatcher;
    private final UserService users;
    private final boolean forwardToExternalAggregator;
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
        opts.setConnectionTimeout(10);
        
        opts.setWill(GUI_ACKS, "{\"status\":\"LB_OFFLINE\"}".getBytes(StandardCharsets.UTF_8), qos, false);

        client.setCallback(new MqttCallbackExtended() {
            @Override 
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println("[MQTT] Connection established to " + serverURI);
                try {
                    client.subscribe(GUI_COMMANDS, qos);
                    System.out.println("[MQTT] Subscribed to " + GUI_COMMANDS);
                } catch (MqttException e) {
                    System.err.println("[MQTT] Subscription failed: " + e.getMessage());
                }
            }

            @Override public void connectionLost(Throwable cause) {
                if (cause != null) System.err.println("[MQTT] Connection lost: " + cause.getMessage());
            }

            @Override public void messageArrived(String topic, MqttMessage message) throws Exception {
                handleIncoming(topic, message);
            }

            @Override public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        dispatcher.addListener(this);

        try {
            System.out.println("[MQTT] Initializing connection to " + brokerUrl + "...");
            client.connect(opts);
        } catch (MqttException e) {
            System.err.println("[MQTT] Broker offline. Reconnect logic active.");
        }
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
                publish(GUI_ACKS, Json.stringify(Map.of("status", "UNAUTHORIZED", "user", user != null ? user : "unknown")));
                return;
            }

            JobType type = JobType.valueOf(typeStr.toUpperCase());
            byte[] data = (dataB64 != null) ? Base64.getDecoder().decode(dataB64) : null;
            Job job = new Job(type, u.name, filename, data, sizeKB, priority);

            if (forwardToExternalAggregator) {
                publish(AGGREGATOR_COMMANDS, payload);
                publish(GUI_ACKS, Json.stringify(Map.of("jobId", job.id, "status", "FORWARDED")));
            } else {
                dispatcher.submit(job);
            }
        } catch (Exception e) {
            System.err.println("[MQTT] Error: " + e.getMessage());
        }
    }

    private void publish(String topic, String payload) {
        try {
            if (client != null && client.isConnected()) {
                client.publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, false);
            }
        } catch (MqttException e) {
            System.err.println("[MQTT] Publish failed: " + e.getMessage());
        }
    }

    @Override public void onQueued(Job job) {
        publish(GUI_ACKS, Json.stringify(Map.of("jobId", job.id, "status", "QUEUED", "filename", job.filename)));
    }
    @Override public void onStarted(Job job) { 
        publish(GUI_RESULTS_PREFIX + job.id, Json.stringify(Map.of("jobId", job.id, "status", "STARTED"))); 
    }
    @Override public void onCompleted(Job job) { 
        publish(GUI_RESULTS_PREFIX + job.id, Json.stringify(Map.of("jobId", job.id, "status", "COMPLETED"))); 
    }
    @Override public void onFailed(Job job, Throwable error) { 
        publish(GUI_RESULTS_PREFIX + job.id, Json.stringify(Map.of("jobId", job.id, "status", "FAILED", "error", error.getMessage()))); 
    }

    static class Json {
        static Map<String,Object> parse(String s) {
            Map<String,Object> map = new java.util.LinkedHashMap<>();
            s = s.trim();
            if (s.startsWith("{")) s = s.substring(1);
            if (s.endsWith("}")) s = s.substring(0, s.length()-1);
            for (String part : s.split(",")) {
                String[] kv = part.split(":", 2);
                if (kv.length < 2) continue;
                String k = kv[0].trim().replace("\"", "");
                String v = kv[1].trim().replace("\"", "");
                map.put(k, v);
            }
            return map;
        }
        static int intOr(Object o, int def) {
            try { return Integer.parseInt(o.toString()); } catch (Exception e) { return def; }
        }
        static String stringify(Map<String,Object> m) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (var e : m.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
                first = false;
            }
            return sb.append("}").toString();
        }
    }
}