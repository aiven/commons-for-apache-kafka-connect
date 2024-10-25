/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.s3.source;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;

import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;

public class ConnectRunnerNew {
    protected EmbeddedConnectCluster connectCluster;

    protected RestApp restApp;

    private final File pluginDir;
    private final String bootstrapServers;
    private final int offsetFlushInterval;

    public ConnectRunnerNew(final File pluginDir, final String bootstrapServers, final int offsetFlushIntervalMs) {
        this.pluginDir = pluginDir;
        this.bootstrapServers = bootstrapServers;
        this.offsetFlushInterval = offsetFlushIntervalMs;
    }

    protected void startConnectCluster(String connectorName, Properties brokerProps) throws Exception {
        Map<String, String> clientConfigs = new HashMap<>();
        clientConfigs.put("bootstrap.servers", brokerProps.getProperty("listeners"));
        connectCluster = new EmbeddedConnectCluster.Builder().name(connectorName)
                .workerProps(getWorkerProperties())
                .build();

        connectCluster.start();
        startSchemaRegistry();
    }

    public String getBootstrapServers() {
        return connectCluster.kafka().bootstrapServers();
    }

    protected void stopConnectCluster() {
        // stop all Connect, Kafka and Zk threads.
        if (connectCluster != null) {
            connectCluster.stop();
            connectCluster = null;
        }
    }

    Map<String, String> getWorkerProperties() throws IOException {
        final Map<String, String> workerProps = new HashMap<>();
        final File tempFile = File.createTempFile("connect", "offsets");
//         workerProps.put("bootstrap.servers", bootstrapServers);

        workerProps.put("offset.flush.interval.ms", Integer.toString(offsetFlushInterval));

        // These don't matter much (each connector sets its own converters), but need to be filled with valid classes.
        workerProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.key.converter.schemas.enable", "true");
        workerProps.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("internal.value.converter.schemas.enable", "true");

        // workerProps.put("offset.storage.file.filename", tempFile.getCanonicalPath());

        // workerProps.put("plugin.path", pluginDir.getPath());
        workerProps.put("plugin.discovery", "hybrid_warn");

        return workerProps;
    }

    public String configureConnector(String connName, Map<String, String> connConfig) {
        return connectCluster.configureConnector(connName, connConfig);
    }

    protected void startSchemaRegistry() throws Exception {
        int port = findAvailableOpenPort();
        restApp = new RestApp(port, null, getBootstrapServers(),
            "_schemas", "NONE", true, new Properties());

        restApp.start();
        waitForSchemaRegistryToStart();
    }

    protected void waitForSchemaRegistryToStart() throws InterruptedException {
        TestUtils.waitForCondition(
            () -> restApp.restServer.isRunning(),
            TimeUnit.MINUTES.toMillis(60),
            "Schema-registry server did not start in time."
        );
    }

    private Integer findAvailableOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    protected void stopSchemaRegistry() throws Exception {
        if (restApp != null) {
            restApp.stop();
        }
    }
}
