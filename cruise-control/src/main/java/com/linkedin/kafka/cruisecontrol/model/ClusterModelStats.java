/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Statistic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.UNIT_INTERVAL_TO_PERCENTAGE;


public class ClusterModelStats {
  private final Map<Statistic, Map<Resource, Double>> _resourceUtilizationStats;
  private final Map<Statistic, Double> _potentialNwOutUtilizationStats;
  private final Map<Statistic, Number> _replicaStats;
  private final Map<Statistic, Number> _leaderReplicaStats;
  private final Map<Statistic, Number> _topicReplicaStats;
  private int _numBrokers;
  private int _numAliveBrokers;
  private int _numReplicasInCluster;
  private int _numPartitionsWithOfflineReplicas;
  private int _numTopics;
  private Map<Resource, Integer> _numBalancedBrokersByResource;
  private int _numBrokersUnderPotentialNwOut;
  private BalancingConstraint _balancingConstraint;
  private double[][] _utilizationMatrix;
  private int _numSnapshotWindows;
  private double _monitoredPartitionsRatio;

  /**
   * Constructor for analysis stats.
   */
  ClusterModelStats() {
    _resourceUtilizationStats = new HashMap<>();
    _potentialNwOutUtilizationStats = new HashMap<>();
    _replicaStats = new HashMap<>();
    _leaderReplicaStats = new HashMap<>();
    _topicReplicaStats = new HashMap<>();
    _numBrokers = 0;
    _numReplicasInCluster = 0;
    _numPartitionsWithOfflineReplicas = 0;
    _numTopics = 0;
    _numBrokersUnderPotentialNwOut = 0;
    _numBalancedBrokersByResource = new HashMap<>();
  }

  /**
   * Populate the analysis stats with this cluster and given balancing constraint.
   *
   * @param clusterModel        The state of the cluster.
   * @param balancingConstraint Balancing constraint.
   * @return Analysis stats with this cluster and given balancing constraint.
   */
  ClusterModelStats populate(ClusterModel clusterModel, BalancingConstraint balancingConstraint) {
    _numBrokers = clusterModel.brokers().size();
    _numAliveBrokers = clusterModel.aliveBrokers().size();
    _numTopics = clusterModel.topics().size();
    _balancingConstraint = balancingConstraint;
    utilizationForResources(clusterModel);
    utilizationForPotentialNwOut(clusterModel);
    numForReplicas(clusterModel);
    numForLeaderReplicas(clusterModel);
    numForAvgTopicReplicas(clusterModel);
    _utilizationMatrix = clusterModel.utilizationMatrix();
    _numSnapshotWindows = clusterModel.load().numWindows();
    _monitoredPartitionsRatio = clusterModel.monitoredPartitionsRatio();
    return this;
  }

  /**
   * Get resource utilization stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Map<Resource, Double>> resourceUtilizationStats() {
    return _resourceUtilizationStats;
  }

  /**
   * Get outbound network utilization stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Double> potentialNwOutUtilizationStats() {
    return _potentialNwOutUtilizationStats;
  }

  /**
   * Get replica stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Number> replicaStats() {
    return _replicaStats;
  }

  /**
   * Get leader replica stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Number> leaderReplicaStats() {
    return _leaderReplicaStats;
  }

  /**
   * Get topic replica stats for the cluster instance that the object was populated with.
   */
  public Map<Statistic, Number> topicReplicaStats() {
    return _topicReplicaStats;
  }

  /**
   * Get number of brokers for the cluster instance that the object was populated with.
   */
  public int numBrokers() {
    return _numBrokers;
  }

  /**
   * Get number of replicas for the cluster instance that the object was populated with.
   */
  public int numReplicasInCluster() {
    return _numReplicasInCluster;
  }

  /**
   * Get number of number of partitions with offline replicas in the cluster.
   */
  public int numPartitionsWithOfflineReplicas() {
    return _numPartitionsWithOfflineReplicas;
  }

  /**
   * Get number of topics for the cluster instance that the object was populated with.
   */
  public int numTopics() {
    return _numTopics;
  }

  /**
   * Get number of balanced brokers by resource for the cluster instance that the object was populated with.
   */
  public Map<Resource, Integer> numBalancedBrokersByResource() {
    return _numBalancedBrokersByResource;
  }

  /**
   * Get number of brokers under potential nw out for the cluster instance that the object was populated with.
   */
  public int numBrokersUnderPotentialNwOut() {
    return _numBrokersUnderPotentialNwOut;
  }

  /**
   * This is the utilization matrix generated from {@link ClusterModel#utilizationMatrix()}.
   * @return non-null if populate has been called else this may return null.
   */
  public double[][] utilizationMatrix() {
    return _utilizationMatrix;
  }

  /**
   * Get the monitored partition percentage of this cluster model;
   */
  public double monitoredPartitionsPercentage() {
    return _monitoredPartitionsRatio * UNIT_INTERVAL_TO_PERCENTAGE;
  }

  /**
   * Get the number of snapshot windows used by this cluster model;
   */
  public int numSnapshotWindows() {
    return _numSnapshotWindows;
  }

  /*
   * Return a valid JSON encoded string
   */
  public String getJSONString() {
    Gson gson = new Gson();
    return gson.toJson(getJsonStructure());
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> statMap = new HashMap<>();
    Map<String, Integer> basicMap = new HashMap<>();
    basicMap.put(AnalyzerUtils.BROKERS, numBrokers());
    basicMap.put(AnalyzerUtils.REPLICAS, numReplicasInCluster());
    basicMap.put(AnalyzerUtils.TOPICS, numTopics());
    List<Statistic> cachedStatistic = Statistic.cachedValues();
    // List of all statistics AVG, MAX, MIN, STD
    Map<String, Object> allStatMap = new HashMap<>(cachedStatistic.size());
    for (Statistic stat : cachedStatistic) {
      List<Resource> cachedResources = Resource.cachedValues();
      Map<String, Object> resourceMap = new HashMap<>(cachedResources.size() + 3);
      for (Resource resource : cachedResources) {
        resourceMap.put(resource.resource(), resourceUtilizationStats().get(stat).get(resource));
      }
      resourceMap.put(AnalyzerUtils.POTENTIAL_NW_OUT, potentialNwOutUtilizationStats().get(stat));
      resourceMap.put(AnalyzerUtils.REPLICAS, replicaStats().get(stat));
      resourceMap.put(AnalyzerUtils.LEADER_REPLICAS, leaderReplicaStats().get(stat));
      resourceMap.put(AnalyzerUtils.TOPIC_REPLICAS, topicReplicaStats().get(stat));
      allStatMap.put(stat.stat(), resourceMap);
    }
    statMap.put(AnalyzerUtils.METADATA, basicMap);
    statMap.put(AnalyzerUtils.STATISTICS, allStatMap);
    return statMap;
  }

  /**
   * @return A string representation of the cluster counts including brokers, replicas, and topics.
   */
  public String toStringCounts() {
    return String.format("%d brokers %d replicas %d topics.", numBrokers(), numReplicasInCluster(), numTopics());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Statistic stat : Statistic.cachedValues()) {
      sb.append(String.format("%s:{", stat));
      for (Resource resource : Resource.cachedValues()) {
        sb.append(String.format("%s:%12.3f ", resource, resourceUtilizationStats().get(stat).get(resource)));
      }
      sb.append(String.format("%s:%12.3f %s:%s %s:%s %s:%s}%n",
                              AnalyzerUtils.POTENTIAL_NW_OUT, potentialNwOutUtilizationStats().get(stat),
                              AnalyzerUtils.REPLICAS, replicaStats().get(stat),
                              AnalyzerUtils.LEADER_REPLICAS, leaderReplicaStats().get(stat),
                              AnalyzerUtils.TOPIC_REPLICAS, topicReplicaStats().get(stat)));
    }
    return sb.substring(0, sb.length() - 2);
  }

  /**
   * Generate statistics of utilization for resources among alive brokers in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void utilizationForResources(ClusterModel clusterModel) {
    // Average, maximum, and standard deviation of utilization by resource.
    Map<Resource, Double> avgUtilizationByResource = new HashMap<>();
    Map<Resource, Double> maxUtilizationByResource = new HashMap<>();
    Map<Resource, Double> minUtilizationByResource = new HashMap<>();
    Map<Resource, Double> stDevUtilizationByResource = new HashMap<>();
    for (Resource resource : Resource.cachedValues()) {
      double avgUtilizationPercentage = clusterModel.load().expectedUtilizationFor(resource) / clusterModel.capacityFor(resource);
      double balanceUpperThreshold = avgUtilizationPercentage * _balancingConstraint.resourceBalancePercentage(resource);
      double balanceLowerThreshold = avgUtilizationPercentage * Math.max(0, (2 - _balancingConstraint.resourceBalancePercentage(resource)));
      // Maximum, minimum, and standard deviation utilization for the resource.
      double hottestBrokerUtilization = 0.0;
      double coldestBrokerUtilization = Double.MAX_VALUE;
      double varianceSum = 0.0;
      int numBalancedBrokers = 0;
      for (Broker broker : clusterModel.aliveBrokers()) {
        double utilization = resource.isHostResource() ? broker.host().load().expectedUtilizationFor(resource)
                                                       : broker.load().expectedUtilizationFor(resource);
        double capacity = resource.isHostResource() ? broker.host().capacityFor(resource)
                                                    : broker.capacityFor(resource);
        double utilizationPercentage = utilization / capacity;
        if (utilizationPercentage >= balanceLowerThreshold && utilizationPercentage <= balanceUpperThreshold) {
          numBalancedBrokers++;
        }
        hottestBrokerUtilization = Math.max(hottestBrokerUtilization, utilization);
        coldestBrokerUtilization = Math.min(coldestBrokerUtilization, utilization);
        varianceSum += Math.pow(utilization - avgUtilizationPercentage * capacity, 2);
      }
      _numBalancedBrokersByResource.put(resource, numBalancedBrokers);
      avgUtilizationByResource.put(resource, clusterModel.load().expectedUtilizationFor(resource) / _numAliveBrokers);
      maxUtilizationByResource.put(resource, hottestBrokerUtilization);
      minUtilizationByResource.put(resource, coldestBrokerUtilization);
      stDevUtilizationByResource.put(resource, Math.sqrt(varianceSum / _numAliveBrokers));
    }
    _resourceUtilizationStats.put(Statistic.AVG, avgUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.MAX, maxUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.MIN, minUtilizationByResource);
    _resourceUtilizationStats.put(Statistic.ST_DEV, stDevUtilizationByResource);
  }

  /**
   * Generate statistics of potential network outbound utilization among alive brokers in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void utilizationForPotentialNwOut(ClusterModel clusterModel) {
    // Average, minimum, and maximum: network outbound utilization and replicas in brokers.
    double maxPotentialNwOut = 0.0;
    double minPotentialNwOut = Double.MAX_VALUE;
    double varianceSum = 0.0;
    double potentialNwOutInCluster = clusterModel.aliveBrokers()
                                                 .stream()
                                                 .mapToDouble(b -> clusterModel.potentialLeadershipLoadFor(b.id()).expectedUtilizationFor(Resource.NW_OUT))
                                                 .sum();
    double avgPotentialNwOutUtilizationPct = potentialNwOutInCluster / clusterModel.capacityFor(Resource.NW_OUT);
    double capacityThreshold = _balancingConstraint.capacityThreshold(Resource.NW_OUT);
    for (Broker broker : clusterModel.aliveBrokers()) {
      double brokerUtilization = clusterModel.potentialLeadershipLoadFor(broker.id()).expectedUtilizationFor(Resource.NW_OUT);
      double brokerCapacity = broker.capacityFor(Resource.NW_OUT);

      if (brokerUtilization / brokerCapacity <= capacityThreshold) {
        _numBrokersUnderPotentialNwOut++;
      }
      maxPotentialNwOut = Math.max(maxPotentialNwOut, brokerUtilization);
      minPotentialNwOut = Math.min(minPotentialNwOut, brokerUtilization);
      varianceSum += (Math.pow(brokerUtilization - avgPotentialNwOutUtilizationPct * brokerCapacity, 2));
    }
    _potentialNwOutUtilizationStats.put(Statistic.AVG, potentialNwOutInCluster / _numAliveBrokers);
    _potentialNwOutUtilizationStats.put(Statistic.MAX, maxPotentialNwOut);
    _potentialNwOutUtilizationStats.put(Statistic.MIN, minPotentialNwOut);
    _potentialNwOutUtilizationStats.put(Statistic.ST_DEV, Math.sqrt(varianceSum / _numAliveBrokers));
  }

  /**
   * Generate statistics for replicas in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void numForReplicas(ClusterModel clusterModel) {
    populateReplicaStats(clusterModel,
                         broker -> broker.replicas().size(),
                         _replicaStats);
    _numReplicasInCluster = clusterModel.numReplicas();
    // Set the number of partitions with offline replicas.
    Set<TopicPartition> partitionsWithOfflineReplicas = new HashSet<>();
    for (Replica replica : clusterModel.selfHealingEligibleReplicas()) {
      partitionsWithOfflineReplicas.add(replica.topicPartition());
    }
    _numPartitionsWithOfflineReplicas = partitionsWithOfflineReplicas.size();
  }

  /**
   * Generate statistics for leader replicas in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void numForLeaderReplicas(ClusterModel clusterModel) {
    populateReplicaStats(clusterModel,
                         broker -> broker.leaderReplicas().size(),
                         _leaderReplicaStats);
  }

  /**
   * Generate statistics for replicas of interest in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   * @param numInterestedReplicasFunc function to calculate number of replicas of interest on a broker.
   * @param interestedReplicaStats statistics for replicas of interest.
   */
  private void populateReplicaStats(ClusterModel clusterModel,
                                    Function<Broker, Integer> numInterestedReplicasFunc,
                                    Map<Statistic, Number> interestedReplicaStats) {
    // Average, minimum, and maximum number of replicas of interest in brokers.
    int maxInterestedReplicasInBroker = 0;
    int minInterestedReplicasInBroker = Integer.MAX_VALUE;
    int numInterestedReplicasInCluster = 0;
    for (Broker broker : clusterModel.brokers()) {
      int numInterestedReplicasInBroker = numInterestedReplicasFunc.apply(broker);
      numInterestedReplicasInCluster += numInterestedReplicasInBroker;
      maxInterestedReplicasInBroker = Math.max(maxInterestedReplicasInBroker, numInterestedReplicasInBroker);
      minInterestedReplicasInBroker = Math.min(minInterestedReplicasInBroker, numInterestedReplicasInBroker);
    }
    double avgInterestedReplicas = ((double) numInterestedReplicasInCluster) / _numAliveBrokers;

    // Standard deviation of replicas of interest in alive brokers.
    double varianceForInterestedReplicas = 0.0;
    for (Broker broker : clusterModel.aliveBrokers()) {
      varianceForInterestedReplicas +=
          (Math.pow((double) numInterestedReplicasFunc.apply(broker) - avgInterestedReplicas, 2) / _numAliveBrokers);
    }

    interestedReplicaStats.put(Statistic.AVG, avgInterestedReplicas);
    interestedReplicaStats.put(Statistic.MAX, maxInterestedReplicasInBroker);
    interestedReplicaStats.put(Statistic.MIN, minInterestedReplicasInBroker);
    interestedReplicaStats.put(Statistic.ST_DEV, Math.sqrt(varianceForInterestedReplicas));
  }

  /**
   * Generate statistics for topic replicas in the given cluster.
   *
   * @param clusterModel The state of the cluster.
   */
  private void numForAvgTopicReplicas(ClusterModel clusterModel) {
    _topicReplicaStats.put(Statistic.AVG, 0.0);
    _topicReplicaStats.put(Statistic.MAX, 0);
    _topicReplicaStats.put(Statistic.MIN, Integer.MAX_VALUE);
    _topicReplicaStats.put(Statistic.ST_DEV, 0.0);
    int numAliveBrokers = clusterModel.aliveBrokers().size();
    for (String topic : clusterModel.topics()) {
      int maxTopicReplicasInBroker = 0;
      int minTopicReplicasInBroker = Integer.MAX_VALUE;
      for (Broker broker : clusterModel.brokers()) {
        int numTopicReplicasInBroker = broker.numReplicasOfTopicInBroker(topic);
        maxTopicReplicasInBroker = Math.max(maxTopicReplicasInBroker, numTopicReplicasInBroker);
        minTopicReplicasInBroker = Math.min(minTopicReplicasInBroker, numTopicReplicasInBroker);
      }
      double avgTopicReplicas = ((double) clusterModel.numTopicReplicas(topic)) / numAliveBrokers;

      // Standard deviation of replicas in alive brokers.
      double variance = 0.0;
      for (Broker broker : clusterModel.aliveBrokers()) {
        variance += (Math.pow(broker.numReplicasOfTopicInBroker(topic) - avgTopicReplicas, 2)
            / (double) numAliveBrokers);
      }

      _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() + avgTopicReplicas);
      _topicReplicaStats.put(Statistic.MAX,
          Math.max(_topicReplicaStats.get(Statistic.MAX).intValue(), maxTopicReplicasInBroker));
      _topicReplicaStats.put(Statistic.MIN,
          Math.min(_topicReplicaStats.get(Statistic.MIN).intValue(), minTopicReplicasInBroker));
      _topicReplicaStats.put(Statistic.ST_DEV, (Double) _topicReplicaStats.get(Statistic.ST_DEV) + Math.sqrt(variance));
    }

    _topicReplicaStats.put(Statistic.AVG, _topicReplicaStats.get(Statistic.AVG).doubleValue() / _numTopics);
    _topicReplicaStats.put(Statistic.ST_DEV, _topicReplicaStats.get(Statistic.ST_DEV).doubleValue() / _numTopics);
  }
}
