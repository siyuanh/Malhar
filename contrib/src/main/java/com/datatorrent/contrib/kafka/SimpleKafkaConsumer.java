/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * Simple kafka consumer adaptor used by kafka input operator Properties:<br>
 * <b>timeout</b>: Timeout for connection and ping <br>
 * <b>bufferSize</b>: buffer size of the consumer <br>
 * <b>clientId</b>: client id of the consumer <br>
 * <b>partitionIds</b>: partition id that the consumer want to consume <br>
 * <li>(-1): create #partition threads and consumers to read the topic from different partitions in parallel</li> <br>
 * <b>metadataRefreshInterval</b>: The interval that the monitor thread use to monitor the broker leadership change <br>
 * <b>metadataRetrievalRetry</b>: Maximum retry times for metadata retrieval failures<br>
 * default value is 3 <br>
 * -1: always retry <br>
 * <br>
 *
 * Load balance: <br>
 * <li>Every consumer only connects to leader broker for particular partition once it's created</li> <li>Once leadership
 * change detected(leader broker failure, or server-side reassignment), it switches to the new leader broker</li> <li>
 * For server-side leadership change, see kafka-preferred-replica-election.sh and kafka-reassign-partitions.sh</li> <li>
 * There is ONE separate thread to monitor the leadership for all the partitions of the topic at every
 * #metadataRefreshInterval milliseconds</li> <br>
 * <br>
 * Kafka broker failover: <br>
 * <li>Once broker failure is detected, it waits #metadataRefreshInterval to reconnect to the new leader broker</li> <li>
 * If there are consecutive #metadataRetrievalRetry failures to retrieve the metadata for the topic. It will stop
 * consuming the partition</li> <br>
 *
 * @since 0.9.0
 */
public class SimpleKafkaConsumer extends KafkaConsumer
{

  public SimpleKafkaConsumer()
  {
    super();
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId)
  {
    this(topic, timeout, bufferSize, clientId, null);
  }

  public SimpleKafkaConsumer(String topic, int timeout, int bufferSize, String clientId, Set<KafkaPartition> partitionIds)
  {
    super(topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.kps = partitionIds;
  }

  public SimpleKafkaConsumer(SetMultimap<String, String> zks, String topic, int timeout, int bufferSize, String clientId, Set<KafkaPartition> partitionIds)
  {
    super(zks, topic);
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.kps = partitionIds;
  }

  private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

  /**
   * Track consumers connected to each broker, topics and partitions hosted on same broker are consumed by same
   * consumer. 
   * Clean the resource if necessary.
   * Key is the broker connection string.
   */
  private final transient Map<String, AtomicReference<SimpleConsumer>> simpleConsumerThreads = new HashMap<String, AtomicReference<SimpleConsumer>>();

  private transient Multimap<String, KafkaPartition> kafkaPartitionsOfBroker = LinkedListMultimap.<String, KafkaPartition> create();

  private transient ExecutorService kafkaConsumerExecutor;

  private transient ScheduledExecutorService metadataRefreshExecutor;

  /**
   * The metadata refresh retry counter
   */
  private final transient AtomicInteger retryCounter = new AtomicInteger(0);

  private int timeout = 10000;

  /**
   * Default buffer size is 1M
   */
  private int bufferSize = 1024 * 1024;

  /**
   * Default client id prefix is "Kafka_Simple_Client"
   */
  @NotNull
  private String clientId = "Kafka_Simple_Client";

  /**
   * Interval in between refresh the metadata change(broker change) in milliseconds.
   * Metadata refresh guarantees to automatically reconnect to new broker that are new elected as broker host
   * Disable metadata refresh by setting this to -1 
   * WARN: Turning off the refresh will disable auto reconnect to new broker
   */
  private int metadataRefreshInterval = 30000;

  /**
   * Maximum brokers' metadata refresh retry limit. 
   * -1 means unlimited retry
   */
  private int metadataRefreshRetryLimit = -1;

  /**
   * You can setup your particular kafka partitions you want to consume for this consumer client.
   * This can be used to share client and thread and maximize the overall performance. 
   * Null or empty value: consumer will create # threads&clients same as # brokers that host the all partitions of the topic
   * Each thread consumes 1(+) partitions from 1 broker
   */
  private Set<KafkaPartition> kps = new HashSet<KafkaPartition>();

  /**
   * Track offset for each partition, so operator could start from the last serialized state Use ConcurrentHashMap to
   * avoid ConcurrentModificationException without blocking reads when updating in another thread(hashtable or
   * synchronizedmap)
   */
  private final ConcurrentHashMap<KafkaPartition, Long> offsetTrack = new ConcurrentHashMap<KafkaPartition, Long>();

  @Override
  public void create()
  {
    super.create();
    Map<String, List<PartitionMetadata>> partitionMetas = KafkaMetadataUtil.getPartitionsForTopic(brokers, topic);
    boolean defaultSelect = (kps == null) || (kps.size() == 0);

    // if partition ids are null or not specified , find all the partitions for
    // the specific topic else create the consumers of specified partition ids
    for (Entry<String, List<PartitionMetadata>> en : partitionMetas.entrySet()) {
      String clusterId = en.getKey();
      for (PartitionMetadata part : en.getValue()) {
        KafkaPartition kp = new KafkaPartition(clusterId, topic, part.partitionId());

        if (defaultSelect || kps.contains(kp)) {
          if (!simpleConsumerThreads.containsKey(part.leader().getConnectionString())) {
            final String clientName = getClientName(part.leader().host());
            logger.info("Connecting to broker {} [cluster:{}, timeout:{}, buffersize:{}, clientId: {}]", part.leader(), clusterId, timeout, bufferSize, clientName);
            simpleConsumerThreads.put(part.leader().getConnectionString(), new AtomicReference<SimpleConsumer>(new SimpleConsumer(part.leader().host(), part.leader().port(), timeout, bufferSize, clientName)));
          }
          kafkaPartitionsOfBroker.put(part.leader().getConnectionString(), kp);
          stats.updatePartitionStats(kp, part.leader().id(), part.leader().host() + ":" + part.leader().port());
        }
      }
    }

  }

  @Override
  public void start()
  {
    super.start();

    // thread to consume the kafka data
    kafkaConsumerExecutor = Executors.newFixedThreadPool(simpleConsumerThreads.size(), new ThreadFactoryBuilder().setNameFormat("kafka-consumer-" + topic + "-%d").build());

    // background thread to monitor the kafka metadata change
    metadataRefreshExecutor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("kafka-consumer-monitor-" + topic + "-%d").setDaemon(true).build());

    // start one monitor thread to monitor the leader broker change and trigger some action
    if (metadataRefreshInterval > 0) {
      metadataRefreshExecutor.scheduleAtFixedRate(new Runnable() {

        @Override
        public void run()
        {
          if (isAlive && (metadataRefreshRetryLimit == -1 || retryCounter.get() < metadataRefreshRetryLimit)) {
            logger.debug("{}: Update metadata for topic {}", Thread.currentThread().getName(), topic);
            Map<String, List<PartitionMetadata>> pms = KafkaMetadataUtil.getPartitionsForTopic(brokers, topic);
            if (pms == null) {
              // retrieve metadata fail
              retryCounter.getAndAdd(1);
            }
            SetMultimap<String, String> newBrokerSet = HashMultimap.<String, String> create();
            Multimap<String, KafkaPartition> latestKafkaPartitionsOfBroker = LinkedListMultimap.<String, KafkaPartition> create();
            for (Entry<String, List<PartitionMetadata>> pmLEntry : pms.entrySet()) {
              for (PartitionMetadata pm : pmLEntry.getValue()) {
                for (Broker b : pm.isr()) {
                  newBrokerSet.put(pmLEntry.getKey(), b.host() + ":" + b.port());
                }
                KafkaPartition kp = new KafkaPartition(pmLEntry.getKey(), topic, pm.partitionId());
                if (!simpleConsumerThreads.containsKey(pm.leader().getConnectionString())) {
                  // connect to new broker if it requires
                  String clientName = getClientName(pm.leader().getConnectionString());
                  logger.info("Connecting to new broker {} [timeout:{}, buffersize:{}, clientId: {}]", pm.leader().toString(), timeout, bufferSize, clientName);
                  simpleConsumerThreads.put(pm.leader().getConnectionString(), new AtomicReference<SimpleConsumer>(new SimpleConsumer(pm.leader().host(), pm.leader().port(), timeout, bufferSize, clientName)));
                }
                latestKafkaPartitionsOfBroker.put(pm.leader().getConnectionString(), kp);
                // always update the latest connection information
                stats.updatePartitionStats(kp, pm.leader().id(), pm.leader().host() + ":" + pm.leader().port());
              }

            }

            // Check if one consumer is not used any more
            for (String broker : kafkaPartitionsOfBroker.keySet()) {
              if (!latestKafkaPartitionsOfBroker.containsKey(broker)) {
                // found a broker not used any more clean the resource
                simpleConsumerThreads.remove(broker).get().close();
              }
            }

            // flip to new broker memory footprint
            brokers = newBrokerSet;
            kafkaPartitionsOfBroker = latestKafkaPartitionsOfBroker;
            // reset to 0 if it reconnect to the broker which has current broker metadata
            retryCounter.set(0);
          }
        }
      }, 0, metadataRefreshInterval, TimeUnit.MILLISECONDS);
    }

    for (final String broker : simpleConsumerThreads.keySet()) {
      // initialize the stats snapshot for this partition
      for (KafkaPartition kp : kafkaPartitionsOfBroker.get(broker)) {
        statsSnapShot.mark(kp, 0);
      }

      final String clientName = getClientName(broker);
      kafkaConsumerExecutor.submit(new Runnable() {

        AtomicReference<SimpleConsumer> csInThreadRef = simpleConsumerThreads.get(broker);

        @Override
        public void run()
        {

          Collection<KafkaPartition> kpl = kafkaPartitionsOfBroker.get(broker);
          // Initialize all start offsets for all kafka partitions read from this consumer
          // read either from beginning of the broker or last offset committed by the operator
          for (KafkaPartition kpForConsumer : kpl) {
            if (offsetTrack.get(kpForConsumer) != null) {
              // start from recovery
//              offsets.put(kpForConsumer, offsetTrack.get(kpForConsumer));
              logger.debug("Partition {} intial offset {}", kpForConsumer, offsetTrack.get(kpForConsumer));
            } else {
              long startOffsetReq = initialOffset.equalsIgnoreCase("earliest") ? OffsetRequest.EarliestTime() : OffsetRequest.LatestTime();
              logger.debug("Partition {} initial offset {} {}", kpForConsumer.getPartitionId(), startOffsetReq, initialOffset);
              offsetTrack.put(kpForConsumer, KafkaMetadataUtil.getLastOffset(csInThreadRef.get(), topic, kpForConsumer.getPartitionId(), startOffsetReq, clientName));
            }
          }

          try {
            // stop consuming only when the consumer container is stopped or the metadata can not be refreshed
            while (isAlive && (metadataRefreshRetryLimit == -1 || retryCounter.get() < metadataRefreshRetryLimit)) {
              try {

                FetchRequestBuilder frb = new FetchRequestBuilder().clientId(clientName);
                // add all partition request in one Fretch request together
                for (KafkaPartition kpForConsumer : kpl) {
                  frb.addFetch(topic, kpForConsumer.getPartitionId(), offsetTrack.get(kpForConsumer), bufferSize);
                }
                
                FetchRequest req = frb.build();
                SimpleConsumer sc = csInThreadRef.get();
                if (sc == null) {
                  if (metadataRefreshInterval > 0) {
                    Thread.sleep(metadataRefreshInterval + 1000);
                  } else {
                    Thread.sleep(100);
                  }
                }
                FetchResponse fetchResponse = csInThreadRef.get().fetch(req);
                for (KafkaPartition kpForConsumer : kpl) {
                  if (fetchResponse.hasError() && fetchResponse.errorCode(topic, kpForConsumer.getPartitionId()) == ErrorMapping.OffsetOutOfRangeCode()) {
                    // If OffsetOutOfRangeCode happen, it means all msgs have been consumed, clean the consumer and
                    // return
                    kafkaPartitionsOfBroker.remove(broker, kpForConsumer);
                    stats.updatePartitionStats(kpForConsumer, -1, "");
                    return;
                  } else if (fetchResponse.hasError()) {
                    // If error happen, assume
                    throw new Exception("Fetch message error, try to reconnect to broker");
                  }
                  long offset = -1l;
                  for (MessageAndOffset msg : fetchResponse.messageSet(topic, kpForConsumer.getPartitionId())) {
                    offset = msg.nextOffset();
                    putMessage(kpForConsumer, msg.message());
                  }
                  if(offset != -1){
                    offsetTrack.put(kpForConsumer, offset);
                  }
                }

              } catch (Exception e) {
                logger.error("The consumer encounters an exception. Close the connection to broker {} \n Caused by {}", broker, e);
                csInThreadRef.get().close();
                for (KafkaPartition kafkaPartition : kpl) {
                  stats.updatePartitionStats(kafkaPartition, -1, "");
                }

              }
            }
          } finally {
            // close the consumer
            if (csInThreadRef.get() != null) {
              csInThreadRef.get().close();
            }
            logger.info("Exit the consumer thread for broker {} ", broker);
          }
        }

      });
    }
  }

  @Override
  public void close()
  {
    logger.info("Stop all consumer threads");
    for (AtomicReference<SimpleConsumer> simConsumerRef : simpleConsumerThreads.values()) {
      if (simConsumerRef.get() != null) {
        simConsumerRef.get().close();
      }
    }
    simpleConsumerThreads.clear();
    metadataRefreshExecutor.shutdownNow();
    kafkaConsumerExecutor.shutdownNow();
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public String getClientId()
  {
    return clientId;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public int getMetadataRefreshInterval()
  {
    return metadataRefreshInterval;
  }

  public void setMetadataRefreshInterval(int reconnectInterval)
  {
    this.metadataRefreshInterval = reconnectInterval;
  }

  public int getMetadataRefreshRetryLimit()
  {
    return metadataRefreshRetryLimit;
  }

  public void setMetadataRefreshRetryLimit(int metadataRefreshRetryLimit)
  {
    this.metadataRefreshRetryLimit = metadataRefreshRetryLimit;
  }

  @Override
  protected KafkaConsumer cloneConsumer(Set<KafkaPartition> partitionIds, Map<KafkaPartition, Long> startOffset)
  {
    // create different client for same partition
    SimpleKafkaConsumer skc = new SimpleKafkaConsumer(zookeeper, topic, timeout, bufferSize, clientId, partitionIds);
    skc.setCacheSize(getCacheSize());
    skc.setMetadataRefreshInterval(getMetadataRefreshInterval());
    skc.setMetadataRefreshRetryLimit(getMetadataRefreshRetryLimit());
    skc.initialOffset = this.initialOffset;
    skc.resetOffset(startOffset);
    skc.setCacheSize(getCacheSize());
    return skc;
  }

  @Override
  protected KafkaConsumer cloneConsumer(Set<KafkaPartition> partitionIds)
  {
    return cloneConsumer(partitionIds, null);
  }

  @Override
  protected void commitOffset()
  {
    // the simple consumer offset is kept in the offsetTrack
    // It's better to do server registry for client in the future. Wait for kafka community come up with more
    // sophisticated offset management
    // TODO https://cwiki.apache.org/confluence/display/KAFKA/Inbuilt+Consumer+Offset+Management#
  }

  private String getClientName(String brokerName)
  {
    return clientId + SIMPLE_CONSUMER_ID_SUFFIX + brokerName;
  }

  @Override
  protected Map<KafkaPartition, Long> getCurrentOffsets()
  {
    return offsetTrack;
  }

  private void resetOffset(Map<KafkaPartition, Long> overrideOffset)
  {
    if (overrideOffset == null) {
      return;
    }
    offsetTrack.clear();
    // set offset of the partitions assigned to this consumer
    for (KafkaPartition kp : kps) {
      Long offsetForPar = overrideOffset.get(kp);
      if (offsetForPar != null) {
        offsetTrack.put(kp, offsetForPar);
      }
    }
  }

  @Override
  public KafkaMeterStats getConsumerStats()
  {
    stats.updateOffsets(offsetTrack);
    return super.getConsumerStats();
  }


} // End of SimpleKafkaConsumer
