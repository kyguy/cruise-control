/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for fetching Kafka cluster metadata using Kafka Admin APIs.
 *
 * This replaces the use of MetadataClient which relies on internal Kafka APIs.
 */
public class MetadataAdminClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataAdminClient.class);

  private final Admin _adminClient;
  private volatile Cluster _cachedCluster;

  /**
   * Creates a new MetadataAdminClient.
   *
   * @param adminClient The AdminClient to use for fetching cluster metadata.
   */
  public MetadataAdminClient(Admin adminClient) {
    _adminClient = adminClient;
  }

  /**
   * Fetches the current metadata for the Kafka cluster.
   * Falls back to cached metadata if the refresh fails.
   *
   * @return a {@link Cluster} containing the cluster ID, broker nodes, and partition information for all topics
   */
  public Cluster cluster() {
    try {
      Set<String> topicNames = _adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();

      Map<String, TopicDescription> topicDescriptions = _adminClient.describeTopics(topicNames).allTopicNames().get();

      DescribeClusterResult describeResult = _adminClient.describeCluster();
      Collection<Node> nodes = describeResult.nodes().get();
      String clusterId = describeResult.clusterId().get();
      Set<Integer> liveNodeIds = nodes.stream().map(Node::id).collect(Collectors.toSet());

      List<PartitionInfo> partitionInfos = new ArrayList<>();

      for (TopicDescription desc : topicDescriptions.values()) {
        for (TopicPartitionInfo partInfo : desc.partitions()) {

          Node leader = partInfo.leader();
          List<Node> replicaList = partInfo.replicas();

          Node[] replicas = replicaList.toArray(Node[]::new);
          Node[] isr = partInfo.isr().toArray(Node[]::new);

          // TopicPartitionInfo doesn't expose offline replicas so we derive them from live broker set.
          Node[] offlineReplicas = replicaList.stream()
              .filter(r -> !liveNodeIds.contains(r.id()))
              .toArray(Node[]::new);

          partitionInfos.add(new PartitionInfo(desc.name(), partInfo.partition(), leader, replicas, isr, offlineReplicas));
        }
      }

      _cachedCluster = new Cluster(clusterId, nodes, partitionInfos, Collections.emptySet(), Collections.emptySet());
      return _cachedCluster;
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (_cachedCluster == null) {
          throw new RuntimeException("Failed to refresh cluster metadata and no cached metadata available", e);
        }
        LOG.warn("Interrupted while fetching cluster metadata, using cached cluster metadata", e);
    } catch (ExecutionException e) {
        if (_cachedCluster == null) {
          throw new RuntimeException("Failed to refresh cluster metadata and no cached metadata available", e);
        }
        LOG.warn("ExecutionException while fetching cluster metadata, using cached cluster metadata", e);
    }
    return _cachedCluster;
  }
}
