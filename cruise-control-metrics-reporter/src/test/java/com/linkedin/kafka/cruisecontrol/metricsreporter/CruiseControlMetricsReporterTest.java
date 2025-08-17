/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.KafkaTopicDescriptionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.kafka.KafkaContainer;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.DEFAULT_BOOTSTRAP_SERVERS_HOST;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.DEFAULT_BOOTSTRAP_SERVERS_PORT;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.getTopicDescription;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CruiseControlMetricsReporterTest extends CCKafkaClientsIntegrationTestHarness {
  private static final String KAFKA_VERSION = "3.9.1";
  private static final int NUM_OF_BROKERS = 2;
  protected static final String TOPIC = "CruiseControlMetricsReporterTest";
  private static final String HOST = "127.0.0.1";
  protected CCEmbeddedKraftCluster _cluster;

  /**
   * Setup the unit test.
   */
  @Before
  public void setUp() {
    List<Map<Object, Object>> brokerConfigs = buildBrokerConfigs();
    _cluster = new CCEmbeddedKraftCluster(KAFKA_VERSION, NUM_OF_BROKERS, brokerConfigs);
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();

    Properties props = new Properties();
    props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    AtomicInteger failed = new AtomicInteger(0);
    try (Producer<String, String> producer = createProducer(props)) {
      for (int i = 0; i < 10; i++) {
        producer.send(new ProducerRecord<>("TestTopic", Integer.toString(i)), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              failed.incrementAndGet();
            }
          }
        });
      }
    }
    assertEquals(0, failed.get());
  }

  /**
   * Cleans up resources after each test method execution.
   */
  @After
  public void tearDown() {
      _cluster.close();
  }

  @Override
  public Properties overridingProps() {
    Properties props = new Properties();
    props.setProperty("listeners", "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094,TC-0://0.0.0.0:9095");
    props.setProperty("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,TC-0:PLAINTEXT");
    props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
    props.setProperty(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), HOST + ":9093");
    props.setProperty(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
    props.setProperty(CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
    props.setProperty("log.flush.interval.messages", "1");
    props.setProperty("offsets.topic.replication.factor", "1");
    props.setProperty("default.replication.factor", "2");

    return props;
  }

  @Test
  public void testReportingMetrics() {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testReportingMetrics");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    setSecurityConfigs(props, "consumer");
    Consumer<String, CruiseControlMetric> consumer = new KafkaConsumer<>(props);

    consumer.subscribe(Collections.singleton(TOPIC));
    long startMs = System.currentTimeMillis();
    HashSet<Integer> expectedMetricTypes = new HashSet<>(Arrays.asList((int) ALL_TOPIC_BYTES_IN.id(),
                                                                       (int) ALL_TOPIC_BYTES_OUT.id(),
                                                                       (int) TOPIC_BYTES_IN.id(),
                                                                       (int) TOPIC_BYTES_OUT.id(),
                                                                       (int) PARTITION_SIZE.id(),
                                                                       (int) BROKER_CPU_UTIL.id(),
                                                                       (int) ALL_TOPIC_REPLICATION_BYTES_IN.id(),
                                                                       (int) ALL_TOPIC_REPLICATION_BYTES_OUT.id(),
                                                                       (int) ALL_TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) ALL_TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) TOPIC_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) TOPIC_FETCH_REQUEST_RATE.id(),
                                                                       (int) TOPIC_MESSAGES_IN_PER_SEC.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_RATE.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_RATE.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_RATE.id(),
                                                                       (int) BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT.id(),
                                                                       (int) BROKER_REQUEST_QUEUE_SIZE.id(),
                                                                       (int) BROKER_RESPONSE_QUEUE_SIZE.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_LOG_FLUSH_RATE.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_MAX.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_MEAN.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_PRODUCE_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH.id(),
                                                                       (int) BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_50TH.id(),
                                                                       (int) BROKER_LOG_FLUSH_TIME_MS_999TH.id()));
    Set<Integer> metricTypes = new HashSet<>();
    ConsumerRecords<String, CruiseControlMetric> records;
    while (metricTypes.size() < expectedMetricTypes.size() && System.currentTimeMillis() < startMs + 15000) {
      records = consumer.poll(Duration.ofMillis(10L));
      for (ConsumerRecord<String, CruiseControlMetric> record : records) {
        metricTypes.add((int) record.value().rawMetricType().id());
      }
    }
    assertEquals("Expected " + expectedMetricTypes + ", but saw " + metricTypes, expectedMetricTypes, metricTypes);
  }

  /**
   * Waits until the metadata of a Kafka topic changes compared to a previously known description.
   *
   * @param adminClient          The Kafka AdminClient instance used to fetch topic metadata.
   * @param oldTopicDescription  The original TopicDescription to compare against.
   * @param timeout              The maximum time to wait for a metadata change.
   *
   * @return The updated TopicDescription once a change is detected.
   */
  private TopicDescription waitForTopicMetadataChange(AdminClient adminClient,
                                                      TopicDescription oldTopicDescription,
                                                      Duration timeout) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    String topicName = oldTopicDescription.name();
    int oldPartitionCount = oldTopicDescription.partitions().size();

    while (System.currentTimeMillis() < deadline) {
      try {
        TopicDescription newTopicDescription = adminClient
          .describeTopics(Collections.singleton(topicName))
          .all()
          .get(timeout.toMillis(), TimeUnit.MILLISECONDS)
          .get(topicName);

        int newPartitionCount = newTopicDescription.partitions().size();

        if (newPartitionCount != oldPartitionCount) {
          return newTopicDescription;
        }

        Thread.sleep(500);
      } catch (ExecutionException | TimeoutException e) {
        // transient error, retry
      }
    }

    throw new RuntimeException("Timeout waiting for topic metadata change for topic: " + topicName);
  }

  @Test
  public void testUpdatingMetricsTopicConfig() throws InterruptedException {
    Properties props = new Properties();
    setSecurityConfigs(props, "admin");
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _cluster.getExternalBootstrapAddress());
    AdminClient adminClient = AdminClient.create(props);

    // For compatibility with Kafka 4.0 and beyond we must use new API methods.
    TopicDescription topicDescription;
    try {
      topicDescription = getTopicDescription(adminClient, TOPIC);
    } catch (KafkaTopicDescriptionException e) {
      throw new RuntimeException(e);
    }
    assertEquals(1, topicDescription.partitions().size());

    KafkaContainer broker = _cluster.getBrokers().get(0);

    // Shutdown broker
    broker.stop();

    // Change broker config
    Map<Object, Object> brokerConfig = buildBrokerConfigs().get(0);
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "2");
    brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    CCEmbeddedKraftCluster.setBrokerConfigViaEnvVars(broker, brokerConfig);

    // Restart broker
    broker.start();

    // Wait for broker to boot up
    _cluster.waitUntilBrokerIsReady(adminClient, 0, Duration.ofSeconds(30));

    // Wait for topic metadata configuration change to propagate
    TopicDescription newTopicDescription = waitForTopicMetadataChange(adminClient, topicDescription, Duration.ofSeconds(30));

    assertEquals(2, newTopicDescription.partitions().size());
  }

  @Test
  public void testGetKafkaBootstrapServersConfigure() {
    // Test with a "listeners" config with a host
    Map<String, String> brokerConfig = _cluster.getBrokers().get(0).getEnvMap();
    Map<String, Object> listenersMap = Collections.singletonMap("listeners", brokerConfig.get("KAFKA_LISTENERS"));
    String bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    String urlParse = "\\[?([0-9a-zA-Z\\-%._:]*)]?:(-?[0-9]+)";
    Pattern urlParsePattern = Pattern.compile(urlParse);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals("0.0.0.0", bootstrapServers.split(":")[0]);

    // Test with a "listeners" config without a host in the first listener.
    String listeners = "SSL://:1234,PLAINTEXT://myhost:4321";
    listenersMap = Collections.singletonMap("listeners", listeners);
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals("1234", bootstrapServers.split(":")[1]);

    // Test with "listeners" and "port" config together.
    listenersMap = new HashMap<>();
    listenersMap.put("listeners", listeners);
    listenersMap.put("port", "43");
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(listenersMap);
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals("43", bootstrapServers.split(":")[1]);

    // Test with null "listeners" and "port" config.
    bootstrapServers = CruiseControlMetricsReporter.getBootstrapServers(Collections.emptyMap());
    assertTrue(urlParsePattern.matcher(bootstrapServers).matches());
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_HOST, bootstrapServers.split(":")[0]);
    assertEquals(DEFAULT_BOOTSTRAP_SERVERS_PORT, bootstrapServers.split(":")[1]);
  }
}
