
package com.mycompany.loadbalancer;

public class Main {
    public static void main(String[] args) throws Exception {
        int port = 8080;
        int replicationFactor = 2;
        long healthCheckIntervalMs = 3000;
        long lockTryTimeoutMs = 15000;

        java.util.List<FileContainer> containers = new java.util.ArrayList<>();
        containers.add(new FileContainer("c1"));
        containers.add(new FileContainer("c2"));
        containers.add(new FileContainer("c3"));

        // Use local package classes instead of lb.core.*
        SchedulingAlgorithm scheduler = new MultiLevelQueues();
        ContainerPicker picker = new RoundRobinContainerPicker();

        UserService users = new UserService();
        FileCatalog catalog = new FileCatalog(replicationFactor);
        Dispatcher dispatcher = new Dispatcher(scheduler, picker, containers, catalog, lockTryTimeoutMs);
        HealthChecker healthChecker = new HealthChecker(containers, healthCheckIntervalMs);

        dispatcher.start();
        healthChecker.start();

        // Start HTTP API (local package class)
        new HttpServerApp(users, dispatcher, catalog, containers).start(port);

        // Start MQTT gateway
        String brokerUrl = "tcp://localhost:1883";
        boolean forwardToExternalAggregator = false;  // set true if aggregator is a separate microservice
        new MqttGateway(brokerUrl, "lb-gateway-" + java.util.UUID.randomUUID(),
                        dispatcher, users, forwardToExternalAggregator);

        System.out.println("LB + HTTP + MQTT ready.");
    }
}
