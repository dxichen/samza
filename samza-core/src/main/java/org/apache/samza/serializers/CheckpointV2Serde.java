/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.serializers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.checkpoint.StateCheckpointMarkerSerde;
import org.apache.samza.system.SystemStreamPartition;

import static com.google.common.base.Preconditions.*;


/**
 * // TODO HIGH dchen the JSON serialization format is secondary to the fact that this serdes SCMs and CheckpointIDs.
 * // Clarify class name, docs, and relationship to the regular CheckpointSerde.
 * JSON Serde for the {@link CheckpointV2} using the {@link JsonSerdeV2} by converting the Checkpoint to a {@link JsonCheckpoint}
 * The following will be the representation of the data format:
 * <code>
 * {
 *   "checkpointId" : "1614147487244-33577",
 *   "inputOffsets" : {
 *     "SystemStreamPartition [test-system, test-stream, 777]" : {
 *       "system" : "SystemName",
 *       "stream" : "StreamName"
 *       "partition" : "777",
 *       "offset" : "1",
 *     }
 *   },
 *   "stateCheckpointMarkers" : {
 *     "store1" : [ StateCheckpointMarker...]
 *     "store2": [...]
 *   }
 * }
 * </code>
 *
 */
public class CheckpointV2Serde implements Serde<CheckpointV2> {
  private static final StateCheckpointMarkerSerde SCM_SERDE = new StateCheckpointMarkerSerde();

  private final Serde<JsonCheckpoint> jsonCheckpointSerde;

  public CheckpointV2Serde() {
    this.jsonCheckpointSerde = new JsonSerdeV2<>(JsonCheckpoint.class);
  }

  @Override
  public CheckpointV2 fromBytes(byte[] bytes) {
    try {
      JsonCheckpoint jsonCheckpoint = jsonCheckpointSerde.fromBytes(bytes);
      Map<SystemStreamPartition, String> sspOffsets = new HashMap<>();
      Map<String, List<StateCheckpointMarker>> stateCheckpoints = new HashMap<>();

      // TODO HIGH dchen include original message in error messages
      jsonCheckpoint.getInputOffsets().forEach((sspName, m) -> {
        String system = m.get("system");
        checkNotNull(system, "System must be present in JSON-encoded SystemStreamPartition");
        String stream = m.get("stream");
        checkNotNull(stream, "Stream must be present in JSON-encoded SystemStreamPartition");
        String partition = m.get("partition");
        checkNotNull(partition, "Partition must be present in JSON-encoded SystemStreamPartition");
        String offset = m.get("offset");
        checkNotNull(offset, "Offset must be present in JSON-encoded SystemStreamPartition");
        sspOffsets.put(new SystemStreamPartition(system, stream, new Partition(Integer.parseInt(partition))), offset);
      });

      jsonCheckpoint.getStateCheckpointMarkers().forEach((storeName, scms) -> {
        List<StateCheckpointMarker> stateCheckpointMarkers = new ArrayList<>();
        checkArgument(!scms.isEmpty(), "StateCheckpointMarker must be present in Stateful checkpoint");
        scms.forEach((scm) -> {
          stateCheckpointMarkers.add(SCM_SERDE.deserialize(scm));
        });
        stateCheckpoints.put(storeName, stateCheckpointMarkers);
      });

      return new CheckpointV2(CheckpointId.fromString(jsonCheckpoint.getCheckpointId()), sspOffsets, stateCheckpoints);
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while deserializing checkpoint: %s", Arrays.toString(bytes)), e);
    }
  }

  @Override
  public byte[] toBytes(CheckpointV2 checkpoint) {
    try {
      String checkpointId = checkpoint.getCheckpointId().toString();
      Map<String, Map<String, String>> inputOffsets = new HashMap<>();
      Map<String, List<String>> storeStateCheckpointMarkers = new HashMap<>();

      // TODO HIGH dchen why does this need to be similar to checkpoint serde when we're doing dual writes?
      // Create input offsets map similar to CheckpointSerde
      // (ssp -> (system, stream, partition, offset))
      checkpoint.getInputOffsets().forEach((ssp, offset) -> {
        Map<String, String> sspOffsetsMap = new HashMap<>();
        sspOffsetsMap.put("system", ssp.getSystem());
        sspOffsetsMap.put("stream", ssp.getStream());
        sspOffsetsMap.put("partition", Integer.toString(ssp.getPartition().getPartitionId()));
        sspOffsetsMap.put("offset", offset);

        inputOffsets.put(ssp.toString(), sspOffsetsMap);
      });

      // Create mapping for state checkpoint markers
      // (storeName -> (StateCheckpointMarkerFactory -> StateCheckpointMarker))
      checkpoint.getStateCheckpointMarkers().forEach((storeName, stateCheckpointMarkers) -> {
        List<String> stateCheckpointMarkerByFactory = new ArrayList<>();
        stateCheckpointMarkers.forEach(stateCheckpointMarker -> {
          // Serialize the StateCheckpointMarker according to StateBackendFactory
          stateCheckpointMarkerByFactory.add(SCM_SERDE.serialize(stateCheckpointMarker));
        });
        storeStateCheckpointMarkers.put(storeName, stateCheckpointMarkerByFactory);
      });

      return jsonCheckpointSerde.toBytes(new JsonCheckpoint(checkpointId, inputOffsets, storeStateCheckpointMarkers));
    } catch (Exception e) {
      throw new SamzaException(String.format("Exception while serializing checkpoint: %s", checkpoint.toString()), e);
    }
  }

  /**
   * Used for Json serialization of the {@link CheckpointV2} class by the
   * {@link CheckpointV2Serde}
   */
  // TODO add documentation for fields and the string-string map contents.
  private class JsonCheckpoint {
    private String checkpointId;
    private Map<String, Map<String, String>> inputOffsets;
    private Map<String, List<String>> stateCheckpointMarkers;

    // Default constructor required for Jackson ObjectMapper
    public JsonCheckpoint() {}

    public JsonCheckpoint(String checkpointId,
        Map<String, Map<String, String>> inputOffsets,
        Map<String, List<String>> stateCheckpointMakers) {
      this.checkpointId = checkpointId;
      this.inputOffsets = inputOffsets;
      this.stateCheckpointMarkers = stateCheckpointMakers;
    }

    public String getCheckpointId() {
      return checkpointId;
    }

    public Map<String, Map<String, String>> getInputOffsets() {
      return inputOffsets;
    }

    public Map<String, List<String>> getStateCheckpointMarkers() {
      return stateCheckpointMarkers;
    }
  }
}
