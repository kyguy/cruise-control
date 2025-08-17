/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The {@code CCEmbeddedKraftCluster} class creates an embedded KRaft Kafka cluster used for testing purposes.
 */
public class CCEmbeddedKraftCluster implements Startable {
  private static final int HOST_PORT_START = 10090;
  private static final int MAPPED_CONTAINER_PORT = 9095;

  private final int _brokersNum;
  private final Network _network;
  private final List<KafkaContainer> _brokers;

  public CCEmbeddedKraftCluster(String brokerVersion, int numOfBrokers, List<Map<Object, Object>> brokerConfigs) {
    if (numOfBrokers < 0) {
      throw new IllegalArgumentException("numOfBrokers '" + numOfBrokers + "' must be greater than 0");
    }

    this._brokersNum = numOfBrokers;
    this._network = Network.newNetwork();

    String controllerQuorumVoters = IntStream
            .range(0, numOfBrokers)
            .mapToObj(brokerNum -> String.format("%d@broker-%d:9094", brokerNum, brokerNum))
            .collect(Collectors.joining(","));

    String clusterId = Uuid.randomUuid().toString();
    this._brokers =
      IntStream
        .range(0, numOfBrokers)
        .mapToObj(brokerNum -> {
          Map<Object, Object> brokerConfig = brokerConfigs.get(brokerNum);

          String networkAlias = "broker-" + brokerNum;
          int externalPort = HOST_PORT_START + brokerNum;

          KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka")
            .withTag(brokerVersion)
            ) {
                /*
                 * At this time, the `configure()` method of the KafkaContainer class overwrites the user configured
                 * Kafka `listener` and `listener.security.protocol.map` configurations, forcing PLAINTEXT protocol for
                 * listeners exposed outside the container network. To allow for TLS connections to the TestContainer
                 * Kafka cluster from clients outside the container network we must override the `configure()` method.
                 *
                 * Tracking issue in TestContainer's Kafka module here:
                 * https://github.com/testcontainers/testcontainers-java/issues/10035
                 */
                @Override
                protected void configure() { }
            }
            .withNetwork(this._network)
            .withNetworkAliases(networkAlias)
            .withExposedPorts(9092, MAPPED_CONTAINER_PORT)
            .withListener("0.0.0.0:" + MAPPED_CONTAINER_PORT, () -> "localhost:" + externalPort)
            .withEnv("CLUSTER_ID", clusterId)
            // Uncomment the following line when debugging Kafka cluster problems.
            //.withLogConsumer(outputFrame -> System.out.print(networkAlias + " | " + outputFrame.getUtf8String()))
            // Uncomment the following line when debugging SSL connection problems.
            //.withEnv("KAFKA_OPTS", "-Djavax.net.debug=ssl,handshake")
            .withStartupTimeout(Duration.ofMinutes(1));
          kafkaContainer.setPortBindings(List.of(externalPort + ":" + MAPPED_CONTAINER_PORT));

          brokerConfig.put("broker.id", brokerNum + "");
          brokerConfig.put("node.id", brokerNum + "");
          brokerConfig.put("controller.quorum.voters", controllerQuorumVoters);

          // TestContainers automatically sets `inter.broker.listener.name` so we must disable `security.inter.broker.protocol`
          // https://kafka.apache.org/documentation/#brokerconfigs_inter.broker.listener.name
          brokerConfig.remove("security.inter.broker.protocol");

          setBrokerConfigViaEnvVars(kafkaContainer, brokerConfig);

          // Mount metrics reporter and generated certs into container
          mount(kafkaContainer, brokerConfig);

          return kafkaContainer;
        })
        .collect(Collectors.toList());
  }

  private void mountCertIfConfigExists(KafkaContainer container, Map<Object, Object> config, String key) {
    if (config.containsKey(key)) {
      String path = config.get(key).toString();
      container.withFileSystemBind(path, path, BindMode.READ_ONLY);
    }
  }

  private void mount(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    Path libsDir = Paths.get("build", "libs").toAbsolutePath().normalize();

    try {
      Path jarPath = Files.list(libsDir)
        .filter(path -> path.getFileName().toString().startsWith("cruise-control-metrics-reporter"))
        .filter(path -> path.getFileName().toString().endsWith(".jar"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Cruise Control Metrics Reporter jar not found in: " + libsDir));

      kafkaContainer.withFileSystemBind(
        jarPath.toString(),
        "/opt/kafka/libs/cruise-control-metrics-reporter.jar",
        BindMode.READ_ONLY
      );

      mountCertIfConfigExists(kafkaContainer, brokerConfig, "ssl.truststore.location");
      mountCertIfConfigExists(kafkaContainer, brokerConfig, "ssl.keystore.location");
      mountCertIfConfigExists(kafkaContainer, brokerConfig, "cruise.control.metrics.reporter.ssl.keystore.location");
    } catch (IOException e) {
      throw new RuntimeException("Failed to mount Kafka container resources", e);
    }
  }

  /**
   * Updates the broker configuration of the Kafka container.
   *
   * @param kafkaContainer The Kafka container to be updated.
   * @param brokerConfig A map of properties that will override the default broker
   *                        configuration.
   */
  public static void setBrokerConfigViaEnvVars(KafkaContainer kafkaContainer, Map<Object, Object> brokerConfig) {
    for (Map.Entry<Object, Object> entry : brokerConfig.entrySet()) {
      String key = String.valueOf(entry.getKey());
      Object rawValue = entry.getValue();
      String value;

      if (rawValue instanceof Collection) {
        value = String.join(",", ((Collection<?>) rawValue).stream()
          .map(Object::toString)
          .toArray(String[]::new));
      } else if (rawValue instanceof String[]) {
        value = String.join(",", (String[]) rawValue);
      } else if (rawValue instanceof org.apache.kafka.common.config.types.Password) {
        value = ((org.apache.kafka.common.config.types.Password) rawValue).value();
      } else if (rawValue instanceof String) {
        value = (String) rawValue;
      } else {
        value = String.valueOf(rawValue);
      }

      // Convert Kafka broker properties key to env var format: e.g., log.retention.hours -> KAFKA_LOG_RETENTION_HOURS
      String envKey = "KAFKA_" + key.toUpperCase().replace('.', '_');
      kafkaContainer.withEnv(envKey, value);
    }
  }

  /**
   * Returns list of KafkaContainer broker objects within the TestContainer Kafka cluster.
   *
   * @return List of KafkaContainer broker objects within the TestContainer Kafka cluster.
   */
  public List<KafkaContainer> getBrokers() {
    return this._brokers;
  }

  /**
   * Returns a comma-separated list of bootstrap server addresses that are reachable from inside
   * the Docker network used by the TestContainers Kafka cluster.
   *
   * @return A comma-separated list of external bootstrap server addresses in the form {@code host:port}.
   */
  public String getInternalBootstrapAddress() {
    return _brokers.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
  }

  /**
   * Returns a comma-separated list of bootstrap server addresses that are reachable from outside
   * the Docker network used by the TestContainers Kafka cluster.
   *
   * @return A comma-separated list of external bootstrap server addresses in the form {@code host:port}.
   */
  public String getExternalBootstrapAddress() {
    return _brokers.stream()
      .map(broker -> String.format("%s:%d", broker.getHost(), broker.getMappedPort(MAPPED_CONTAINER_PORT)))
      .collect(Collectors.joining(","));
  }

  @Override
  public void start() {
    _brokers.parallelStream().forEach(GenericContainer::start);

    Properties props = new Properties();
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getInternalBootstrapAddress());
    AdminClient adminClient = AdminClient.create(props);

    IntStream.range(0, _brokersNum).parallel().forEach(
      id -> waitUntilBrokerIsReady(adminClient, id, Duration.ofSeconds(30))
    );

    adminClient.close();
  }

  @Override
  public void stop() {
    this._brokers.stream().parallel().forEach(GenericContainer::stop);
  }

  /**
   * Waits until the identified broker is part of the Kafka cluster.
   *
   * @param adminClient The Kafka AdminClient instance.
   * @param brokerId    The id of broker.
   * @param timeout     The maximum duration to wait for the broker to join the cluster.
   * @throws RuntimeException if the broker does not become part of the cluster before the timeout,
   *                          or if the thread is interrupted while waiting.
   */
  public void waitUntilBrokerIsReady(AdminClient adminClient, int brokerId, Duration timeout) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();

    while (System.currentTimeMillis() < deadline) {
      try {
        DescribeClusterResult cluster = adminClient.describeCluster();
        Collection<Node> nodes = cluster.nodes().get();

        boolean found = nodes.stream().anyMatch(node -> node.id() == brokerId);
        if (found) {
          return;
        }

        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for broker to become ready", e);
      } catch (ExecutionException e) {
        // Cluster info might not be ready yet, ignore and retry
      }
    }

    throw new RuntimeException("Broker " + brokerId + " did not become ready within timeout");
  }
}
