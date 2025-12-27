package com.mycompany.loadbalancer;

import java.util.*;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        // 1. Initialize core services
        UserService users = new UserService();
        FileCatalog catalog = new FileCatalog();
        List<FileContainer> containers = Collections.synchronizedList(new ArrayList<>());
        Dispatcher dispatcher = new Dispatcher(catalog, containers);

        // 2. Setup Shutdown Hook (Release ports when app stops)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[System] Shutting down services...");
        }));

        // 3. Start MQTT Gateway (Fails gracefully if broker is offline)
        try {
            System.out.println("Starting MQTT Gateway...");
            // Replace with your Host IP if necessary (e.g., 10.0.2.2 for VirtualBox)
// To this:
String uniqueId = "LB-Client-" + System.currentTimeMillis();
new MqttGateway("tcp://localhost:1883", uniqueId, dispatcher, users, false);        } catch (Exception e) {
            System.err.println("MQTT Gateway failed: " + e.getMessage());
        }

        // 4. Start HTTP Server with Port-Failover
        int port = 8080;
        boolean started = false;
        HttpServerApp httpApp = new HttpServerApp(users, dispatcher, catalog, containers);

        while (!started && port < 8090) {
            try {
                httpApp.start(port);
                started = true;
            } catch (java.net.BindException e) {
                System.err.println("Port " + port + " is busy. Trying " + (port + 1) + "...");
                port++;
            } catch (IOException e) {
                System.err.println("Critical error starting HTTP server: " + e.getMessage());
                break;
            }
        }

        if (started) {
            System.out.println("LB System fully initialized and ready.");
        } else {
            System.err.println("Could not start HTTP server on any port in range 8080-8090.");
        }
    }
}