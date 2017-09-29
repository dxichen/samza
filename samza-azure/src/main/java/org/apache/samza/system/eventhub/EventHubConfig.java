package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class EventHubConfig extends MapConfig {
  public static final String CONFIG_STREAM_LIST = "systems.%s.stream.list";

  public static final String CONFIG_STREAM_NAMESPACE = "systems.%s.streams.%s.eventhubs.namespace";

  public static final String CONFIG_STREAM_ENTITYPATH = "systems.%s.streams.%s.eventhubs.entitypath";

  public static final String CONFIG_STREAM_SAS_KEY_NAME = "systems.%s.streams.%s.eventhubs.sas.keyname";

  public static final String CONFIG_STREAM_SAS_TOKEN = "systems.%s.streams.%s.eventhubs.sas.token";

  public static final String CONFIG_STREAM_SERDE_FACTORY = "systems.%s.streams.%s.eventhubs.serdeFactory";
  public static final String CONFIG_STREAM_SERDE_PREFIX = "systems.%s.streams.%s.eventhubs.serde.";

  public static final String CONFIG_STREAM_CONSUMER_GROUP = "systems.%s.streams.%s.eventhubs.consumer.group";
  public static final String DEFAULT_CONFIG_STREAM_CONSUMER_GROUP = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;

  public static final String CONFIG_STREAM_CONSUMER_START_POSITION = "systems.%s.streams.%s.eventhubs.start.position";
  public static final String DEFAULT_CONFIG_STREAM_CONSUMER_START_POSITION = StartPosition.LATEST.name();

  public static final String CONFIG_PRODUCER_PARTITION_METHOD = "systems.%s.eventhubs.partition.method";
  public static final String DEFAULT_CONFIG_PRODUCER_PARTITION_METHOD = EventHubSystemProducer
          .PartitioningMethod.EVENT_HUB_HASHING.name();

  public static final String CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = "systems.%s.eventhubs.send.key";
  public static final String DEFAULT_CONFIG_SEND_KEY_IN_EVENT_PROPERTIES = Boolean.toString(false);

  public static final String CONFIG_GET_RUNTIME_INFO_TIMEOUT_MILLIS = "systems.%s.eventhubs.getruntime.timeout";
  public static final String DEFAULT_CONFIG_GET_RUNTIME_INFO_TIMEOUT_MILLIS = Integer.toString(1000);

  private final String _system;

  public EventHubConfig(Map<String, String> config, String systemName) {
    super(config);
    _system = systemName;
  }

  /**
   * Get the list of streams that are defined. Each stream has enough
   * information for connecting to a certain EventHub entity.
   *
   * @return list of stream names
   */
  public List<String> getStreamList() {
    return getList(String.format(CONFIG_STREAM_LIST, _system));
  }

  /**
   * Get the EventHubs namespace for the stream
   *
   * @param streamName name of stream
   * @return EventHubs namespace
   */
  public String getStreamNamespace(String streamName) {
    return get(String.format(CONFIG_STREAM_NAMESPACE, _system, streamName));
  }

  /**
   * Get the EventHubs entity path (topic name) for the stream
   *
   * @param streamName name of stream
   * @return EventHubs entity path
   */
  public String getStreamEntityPath(String streamName) {
    return get(String.format(CONFIG_STREAM_ENTITYPATH, _system, streamName));
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) key name for the stream
   *
   * @param streamName name of stream
   * @return EventHubs SAS key name
   */
  public String getStreamSasKeyName(String streamName) {
    return get(String.format(CONFIG_STREAM_SAS_KEY_NAME, _system, streamName));
  }

  /**
   * Get the EventHubs SAS (Shared Access Signature) token for the stream
   *
   * @param streamName name of stream
   * @return EventHubs SAS token
   */
  public String getStreamSasToken(String streamName) {
    return get(String.format(CONFIG_STREAM_SAS_TOKEN, _system, streamName));
  }

  public Optional<Serde<byte[]>> getSerde(String streamName) {
    Serde<byte[]> serde = null;
    String serdeFactoryClassName = this.get(String.format(CONFIG_STREAM_SERDE_FACTORY, _system, streamName));
    if (!StringUtils.isEmpty(serdeFactoryClassName)) {
      SerdeFactory<byte[]> factory = EventHubSystemFactory.getSerdeFactory(serdeFactoryClassName);
      serde = factory.getSerde(streamName, this.subset(String.format(CONFIG_STREAM_SERDE_PREFIX, _system, streamName)));
    }
    return Optional.ofNullable(serde);
  }

  /**
   * Get the EventHubs consumer group used for consumption for the stream
   *
   * @param streamName name of stream
   * @return EventHubs consumer group
   */
  public String getStreamConsumerGroup(String streamName) {
    return get(String.format(CONFIG_STREAM_CONSUMER_GROUP, _system, streamName), DEFAULT_CONFIG_STREAM_CONSUMER_GROUP);
  }

  /**
   * Get the start position when there is no checkpoints. By default the consumer starts from latest (end of stream)
   *
   * @param streamName name of the stream
   * @return Starting position when no checkpoints
   */
  public StartPosition getStartPosition(String streamName) {
    String startPositionStr = get(String.format(CONFIG_STREAM_CONSUMER_START_POSITION, _system, streamName),
            DEFAULT_CONFIG_STREAM_CONSUMER_START_POSITION);
    return StartPosition.valueOf(startPositionStr.toUpperCase());
  }

  /**
   * Get the partition method of the system. By default partitioning is handed by EventHub.
   *
   * @return The method the producer should use to partition the outgoing data
   */
  public EventHubSystemProducer.PartitioningMethod getPartitioningMethod() {
    String partitioningMethod = get(String.format(CONFIG_PRODUCER_PARTITION_METHOD, _system),
            DEFAULT_CONFIG_PRODUCER_PARTITION_METHOD);
    return EventHubSystemProducer.PartitioningMethod.valueOf(partitioningMethod);

  }

  /**
   * Returns true if the OutgoingMessageEnvelope key should be sent in the outgoing envelope, false otherwise
   *
   * @return Boolean, is send key included
   */
  public Boolean getSendKeyInEventProperties() {
    String isSendKeyIncluded = get(String.format(CONFIG_SEND_KEY_IN_EVENT_PROPERTIES, _system),
            DEFAULT_CONFIG_SEND_KEY_IN_EVENT_PROPERTIES);
    return Boolean.valueOf(isSendKeyIncluded);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _system);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) && _system.equals(((EventHubConfig) obj)._system);
  }

  public enum StartPosition {
    EARLIEST,
    LATEST
  }

}
