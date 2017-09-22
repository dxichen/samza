package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import com.microsoft.azure.servicebus.StringUtil;
import org.apache.samza.SamzaException;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

public class EventHubEntityConnection {
    private static final Logger LOG = LoggerFactory.getLogger(EventHubEntityConnection.class);

    private final String _namespace;
    private final String _entityPath;
    private final String _sasKeyName;
    private final String _sasKey;
    private final String _consumerName;
    private final Map<Integer, PartitionReceiver> _receivers = new TreeMap<>();
    private EventHubClientWrapper _ehClientWrapper;
    private boolean _isStarted = false;

    final Map<Integer, String> _offsets = new TreeMap<>();
    final Map<Integer, PartitionReceiveHandler> _handlers = new TreeMap<>();

    EventHubEntityConnection(String namespace, String entityPath, String sasKeyName, String sasKey, String consumerName) {
        _namespace = namespace;
        _entityPath = entityPath;
        _sasKeyName = sasKeyName;
        _sasKey = sasKey;
        _consumerName = consumerName;
    }

    // add partitions and handlers for this connection. This can be called multiple times
    // for multiple partitions, but needs to be called before connectAndStart()
    synchronized void addPartition(int partitionId, String offset, PartitionReceiveHandler handler) {
        if (_isStarted) {
            LOG.warn("Trying to add partition when the connection has already started.");
            return;
        }
        _offsets.put(partitionId, offset);
        _handlers.put(partitionId, handler);
    }

    // establish the connection and start consuming events
    synchronized void connectAndStart() {
        _isStarted = true;
        try {
            LOG.info(String.format("Starting connection for namespace=%s, entity=%s ", _namespace, _entityPath));
            // upon the instantiation of the client, the connection will be established
            _ehClientWrapper =
                    new EventHubClientWrapper(null, 0, _namespace, _entityPath, _sasKeyName, _sasKey);
            for (Map.Entry<Integer, String> entry : _offsets.entrySet()) {
                Integer id = entry.getKey();
                String offset = entry.getValue();
                try {
                    PartitionReceiver receiver;
                    if (StringUtil.isNullOrWhiteSpace(offset)) {
                        throw new SamzaException(
                                String.format("Invalid offset %s namespace=%s, entity=%s", offset, _namespace, _entityPath));
                    }
                    if (offset.equals(EventHubSystemConsumer.END_OF_STREAM)) {
                        receiver = _ehClientWrapper.getEventHubClient()
                                .createReceiverSync(_consumerName, id.toString(), Instant.now());
                    } else {
                        receiver = _ehClientWrapper.getEventHubClient()
                                .createReceiverSync(_consumerName, id.toString(), offset,
                                        !offset.equals(PartitionReceiver.START_OF_STREAM));
                    }
                    receiver.setReceiveHandler(_handlers.get(id));
                    _receivers.put(id, receiver);
                } catch (Exception e) {
                    throw new SamzaException(
                            String.format("Failed to create receiver for EventHubs: namespace=%s, entity=%s, partitionId=%d",
                                    _namespace, _entityPath, id), e);
                }
            }
        } catch (Exception e) {
            throw new SamzaException(
                    String.format("Failed to create connection to EventHubs: namespace=%s, entity=%s", _namespace, _entityPath),
                    e);
        }
        LOG.info(String.format("Connection successfully started for namespace=%s, entity=%s ", _namespace, _entityPath));
    }

    synchronized void stop() {
        LOG.info(String.format("Stopping connection for namespace=%s, entity=%s ", _namespace, _entityPath));
        try {
            for (PartitionReceiver receiver : _receivers.values()) {
                receiver.closeSync();
            }
            _ehClientWrapper.closeSync();
        } catch (ServiceBusException e) {
            throw new SamzaException(
                    String.format("Failed to stop connection for namespace=%s, entity=%s ", _namespace, _entityPath), e);
        }
        _isStarted = false;
        _offsets.clear();
        _handlers.clear();
        LOG.info(String.format("Connection for namespace=%s, entity=%s stopped", _namespace, _entityPath));
    }
}
